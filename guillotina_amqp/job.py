from aiohttp import test_utils
from aiohttp.helpers import noop
from guillotina.auth.participation import GuillotinaParticipation
from guillotina.auth.users import GuillotinaUser
from guillotina.component import get_utility
from guillotina.interfaces import ACTIVE_LAYERS_KEY
from guillotina.interfaces import Allow
from guillotina.interfaces import IAnnotations
from guillotina.interfaces import IApplication
from guillotina.registry import REGISTRY_DATA_KEY
from guillotina.security.policy import Interaction
from guillotina.transactions import abort
from guillotina.transactions import commit
from guillotina.utils import import_class
from guillotina.utils import resolve_dotted_name
from guillotina_amqp.interfaces import ITaskDefinition
from guillotina_amqp.interfaces import MessageType
from guillotina_amqp.state import get_state_manager
from multidict import CIMultiDict
from unittest import mock
from urllib.parse import urlparse
from zope.interface import alsoProvides

import aiotask_context
import inspect
import logging
import yarl
import asyncio
from datetime import datetime


logger = logging.getLogger('guillotina_amqp.job')


def login_user(request, user_data):
    """Logs user in to guillotina so the job has the correct access

    """
    request.security = Interaction(request)
    participation = GuillotinaParticipation(request)
    participation.interaction = None

    if 'id' in user_data:
        user = GuillotinaUser(request)
        user.id = user_data['id']
        user._groups = user_data.get('groups', [])
        user._roles = {name: Allow for name in user_data['roles']}
        user.data = user_data.get('data', {})
        participation.principal = user
        request._cache_user = user

    request.security.add(participation)
    request.security.invalidate_cache()
    request._cache_groups = {}
    if user_data.get('Authorization'):
        request.headers['Authorization'] = user_data['Authorization']


class EmptyPayload:

    async def readany(self):
        return bytearray()

    def at_eof(self):
        return True


class Job:
    """Job objects are responsible for running the actual functions that
    were configured for. They ack/nack rabbitmq when job is finished, and publish

    """

    def __init__(self, base_request, data, channel, envelope):
        if base_request is None:
            from guillotina.tests.utils import make_mocked_request
            base_request = make_mocked_request('POST', '/db')
        self.base_request = base_request
        self.data = data
        self.channel = channel
        self.envelope = envelope
        self.loop = asyncio.get_event_loop()

        self.task = None
        self._state_manager = None

    @property
    def state_manager(self):
        if self._state_manager is None:
            self._state_manager = get_state_manager()
        return self._state_manager

    async def create_request(self):
        req_data = self.data['req_data']
        url = req_data['url']
        parsed = urlparse(url)
        dct = {
            'method': req_data['method'],
            'url': yarl.URL(url),
            'path': parsed.path,
            'headers': CIMultiDict(req_data['headers']),
            'raw_headers': tuple((k.encode('utf-8'), v.encode('utf-8'))
                                 for k, v in req_data['headers'].items())
        }

        message = self.base_request._message._replace(**dct)

        payload_writer = mock.Mock()
        payload_writer.write_eof.side_effect = noop
        payload_writer.drain.side_effect = noop

        protocol = mock.Mock()
        protocol.transport = test_utils._create_transport(None)
        protocol.writer = payload_writer

        request = self.base_request.__class__(
            message,
            EmptyPayload(),
            protocol,
            payload_writer,
            self.task,
            self.task._loop,
            client_max_size=self.base_request._client_max_size,
            state=self.base_request._state.copy())
        aiotask_context.set('request', request)
        request.annotations = req_data.get('annotations', {})

        if self.data.get('db_id'):
            root = get_utility(IApplication, name='root')
            db = await root.async_get(self.data['db_id'])
            request._db_write_enabled = True
            request._db_id = db.id
            # Add a transaction Manager to request
            tm = request._tm = db.get_transaction_manager()
            # Start a transaction
            txn = await tm.begin(request=request)
            # Get the root of the tree
            context = await tm.get_root(txn=txn)

            if self.data.get('container_id'):
                container = await context.async_get(self.data['container_id'])
                if container is None:
                    raise Exception('Could not find container')
                request._container_id = container.id
                request.container = container
                annotations_container = IAnnotations(container)
                request.container_settings = await annotations_container.async_get(REGISTRY_DATA_KEY)
                layers = request.container_settings.get(ACTIVE_LAYERS_KEY, [])
                for layer in layers:
                    try:
                        alsoProvides(request, import_class(layer))
                    except ModuleNotFoundError:
                        pass
        return request

    async def __call__(self):
        request = None
        result = None
        committed = False
        task_id = self.data['task_id']
        dotted_name = self.data['func']
        logger.info(f'Running task: {task_id}: {dotted_name}')

        # Update status
        await self.state_manager.update(self.data['task_id'], {
            'status': 'running'
        })
        # Clone request for task
        request = await self.create_request()

        req_data = self.data['req_data']
        if 'user' in req_data:
            login_user(request, req_data['user'])

        # Parse the function to run
        func = resolve_dotted_name(self.data['func'])
        if ITaskDefinition.providedBy(func):
            func = func.func
        if hasattr(func, '__real_func__'):
            # from decorators
            func = func.__real_func__

        #
        # Run the task coroutine
        #
        # Your coroutine can be a regular coroutine or an asynchronous
        # generator. If you use an asynchronous generator, your generator
        # should yield tuples of the form:
        #
        # (type, content)
        #
        # There are 2 different event types:
        #
        # type = MessageType.RESULT: task value yield
        # type = MessageType.DEBUG: task message, will be logged to the state manager3
        #
        # All the values yielded by the task are returned as a list to the
        # caller
        #

        # Function is an async generator
        if inspect.isasyncgenfunction(func):
            async for status in func(*self.data['args'], **self.data['kwargs']):
                if not isinstance(status, tuple) or len(status) != 2:
                    logger.debug(f'Job: invalid generator event: {status}')
                    continue

                msg_type, content = status

                if msg_type == MessageType.DEBUG:
                    # DEBUG message: record it in the task's event log
                    date_now = datetime.now().isoformat(' ', 'milliseconds')
                    logger.debug(f'Job {task_id}: function data {self.data}, got msg {content}')

                    # Update the status with new log
                    state = await self.state_manager.get(task_id)
                    state.setdefault('eventlog', [])
                    state['eventlog'].append([date_now, content])
                    await self.state_manager.update(task_id, state)

                elif msg_type == MessageType.RESULT:
                    # RESULT value yielded: accumulate all the
                    # generator results in a list
                    logger.debug(f'Job {task_id}: function data {self.data}, got result {content}')
                    if result is None:
                        result = [content]
                    elif isinstance(result, list):
                        result.append(content)
                else:
                    # Unknown message: log and continue
                    logger.debug(f'Job {task_id}: invalid generator event code {msg_type}')
                    continue
        else:
            # Regular coroutine
            result = await func(*self.data['args'], **self.data['kwargs'])

        # Finish and return result
        await commit(request)
        committed = True
        if request is not None and not committed:
            await abort(request)
        return result
