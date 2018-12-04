from guillotina import app_settings
from guillotina.interfaces import Allow
from guillotina.interfaces import IAbsoluteURL
from guillotina.utils import get_content_path
from guillotina.utils import get_current_request
from guillotina.utils import get_dotted_name
from guillotina.utils import navigate_to
from guillotina.utils import resolve_dotted_name
from guillotina_amqp import amqp
from guillotina_amqp.interfaces import ITaskDefinition
from guillotina_amqp.state import get_state_manager
from guillotina_amqp.state import TaskState

import inspect
import aioamqp
import json
import logging
import time
import uuid
import asyncio


logger = logging.getLogger('guillotina_amqp.utils')


async def cancel_task(task_id):
    """It cancels a task by id. Returns wether it could be cancelled.

    """
    task = TaskState(task_id)
    success = await task.cancel()
    return success


async def add_task(func, *args, _request=None, _retries=3, **kwargs):
    """Given a function and its arguments, it adds it as a task to be ran
    by workers.
    """
    # Get the request and prepare request data
    if _request is None:
        _request = get_current_request()

    req_data = {
        'url': str(_request.url),
        'headers': dict(_request.headers),
        'method': _request.method,
        'annotations': getattr(_request, 'annotations', {})
    }
    try:
        participation = _request.security.participations[0]
        user = participation.principal
        req_data['user'] = {
            'id': user.id,
            'roles': [name for name, setting in user.roles.items()
                      if setting == Allow],
            'groups': user.groups,
            'Authorization': _request.headers.get('Authorization'),
            'data': getattr(user, 'data', {})
        }
    except (AttributeError, IndexError):
        pass

    if getattr(_request, 'container', None):
        req_data['container_url'] = IAbsoluteURL(_request.container, _request)()

    retries = 0
    while True:
        # Get the rabbitmq connection
        channel, transport, protocol = await amqp.get_connection()
        try:
            task_id = str(uuid.uuid4())
            state = TaskState(task_id)
            dotted_name = get_dotted_name(func)
            logger.info(f'Scheduling task: {task_id}: {dotted_name}')
            data = json.dumps({
                'func': dotted_name,
                'args': args,
                'kwargs': kwargs,
                'db_id': getattr(_request, '_db_id', None),
                'container_id': getattr(_request, '_container_id', None),
                'req_data': req_data,
                'task_id': task_id
            })
            # Publish task data on rabbitmq
            await channel.publish(
                data,
                exchange_name=app_settings['amqp']['exchange'],
                routing_key=app_settings['amqp']['queue'],
                properties={
                    'delivery_mode': 2
                }
            )
            # Update tasks's global state
            state_manager = get_state_manager()
            await state_manager.update(task_id, {
                'status': 'scheduled',
                'updated': time.time()
            })
            logger.info(f'Scheduled task: {task_id}: {dotted_name}')
            return state
        except (aioamqp.AmqpClosedConnection, aioamqp.exceptions.ChannelClosed):
            await amqp.remove_connection()
            if retries >= _retries:
                raise
            retries += 1


async def _prepare_func(dotted_func, path, *args, **kwargs):
    request = get_current_request()
    ob = await navigate_to(request.container, path)
    func = resolve_dotted_name(dotted_func)
    if ITaskDefinition.providedBy(func):
        func = func.func
    return ob, func


async def _run_object_task(dotted_func, path, *args, **kwargs):
    ob, func = await _prepare_func(dotted_func, path, *args, **kwargs)
    return await func(ob, *args, **kwargs)


async def _yield_object_task(dotted_func, path, *args, **kwargs):
    ob, func = await _prepare_func(dotted_func, path, *args, **kwargs)
    async for res in func(ob, *args, **kwargs):
        yield res


async def add_object_task(callable=None, ob=None, *args,
                          _request=None, _retries=3, **kwargs):
    superfunc = _run_object_task
    if inspect.isasyncgenfunction(callable):
        # async generators need to be yielded from
        superfunc = _yield_object_task
    return await add_task(
        superfunc, get_dotted_name(callable), get_content_path(ob), *args,
        _request=_request, _retries=_retries, **kwargs)


class TimeoutLock(object):
    """Implements a Lock that can be acquired for """
    def __init__(self, worker_id):
        self._lock = asyncio.Lock()
        self.worker_id = worker_id

    async def acquire(self, ttl=-1):
        """If ttl is -1, lock will be acquired forever (or until someone
        manually releases it).

        Otherwise, it schedules an automatic release after the
        specified ttl.
        """
        if self.locked():
            # Already acquired
            raise False

        acquired = await self._lock.acquire()
        if not acquired:
            return False

        if ttl >= 0:
            asyncio.ensure_future(self._release_after(ttl))
        return True

    async def _release_after(self, some_time):
        await asyncio.sleep(some_time)
        self.release()

    def release(self):
        if self.locked():
            self._lock.release()

    def locked(self):
        return self._lock.locked()

    async def refresh_lock(self, ttl):
        # Overwrite old lock and acquire with new timeout
        self._lock = asyncio.Lock()
        return await self.acquire(ttl)
