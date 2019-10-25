from aiohttp import test_utils
from aiohttp.helpers import noop
from datetime import datetime
from guillotina import glogging
from guillotina import task_vars as g_task_vars
from guillotina.auth.users import GuillotinaUser
from guillotina.auth.utils import set_authenticated_user
from guillotina.component import get_utility
from guillotina.interfaces import ACTIVE_LAYERS_KEY
from guillotina.interfaces import Allow
from guillotina.interfaces import IAnnotations
from guillotina.interfaces import IApplication
from guillotina.registry import REGISTRY_DATA_KEY
from guillotina.transactions import abort
from guillotina.transactions import commit
from guillotina.utils import get_dotted_name
from guillotina.utils import import_class
from guillotina.utils import resolve_dotted_name
from guillotina_amqp import task_vars
from guillotina_amqp.exceptions import ObjectNotFoundException
from guillotina_amqp.interfaces import ITaskDefinition
from guillotina_amqp.interfaces import MessageType
from guillotina_amqp.state import get_state_manager
from guillotina_amqp.state import update_task_running
from guillotina_amqp.utils import _run_object_task
from guillotina_amqp.utils import _yield_object_task
from multidict import CIMultiDict
from unittest import mock
from urllib.parse import urlparse
from zope.interface import alsoProvides

import inspect
import time
import yarl


logger = glogging.getLogger("guillotina_amqp.job")


def login_user(request, user_data):
    """Logs user in to guillotina so the job has the correct access

    """
    if "id" in user_data:
        user = GuillotinaUser(
            user_id=user_data["id"],
            groups=user_data.get("groups", []),
            roles={name: Allow for name in user_data["roles"]},
        )
        user.data = user_data.get("data", {})
        set_authenticated_user(user)
    else:
        set_authenticated_user(None)

    if user_data.get("Authorization"):
        # leave in for b/w compat, remove at later date
        request.headers["Authorization"] = user_data["Authorization"]
    for name, value in (user_data.get("headers") or {}).items():
        request.headers[name] = value


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

            base_request = make_mocked_request("POST", "/db")
        self.base_request = base_request
        self.data = data
        self.channel = channel
        self.envelope = envelope

        self.task = None
        self._state_manager = None
        self._started = time.time()

    @property
    def state_manager(self):
        if self._state_manager is None:
            self._state_manager = get_state_manager()
        return self._state_manager

    async def create_request(self):
        req_data = self.data["req_data"]
        url = req_data["url"]
        parsed = urlparse(url)
        dct = {
            "method": req_data["method"],
            "url": yarl.URL(url),
            "path": parsed.path,
            "headers": CIMultiDict(req_data["headers"]),
            "raw_headers": tuple(
                (k.encode("utf-8"), v.encode("utf-8"))
                for k, v in req_data["headers"].items()
            ),
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
            state=self.base_request._state.copy(),
        )
        g_task_vars.request.set(request)
        request.annotations = req_data.get("annotations", {})

        if self.data.get("db_id"):
            root = get_utility(IApplication, name="root")
            db = await root.async_get(self.data["db_id"])
            g_task_vars.db.set(db)
            # Add a transaction Manager to request
            tm = db.get_transaction_manager()
            g_task_vars.tm.set(tm)
            # Start a transaction
            txn = await tm.begin()
            # Get the root of the tree
            context = await tm.get_root(txn=txn)

            if self.data.get("container_id"):
                container = await context.async_get(self.data["container_id"])
                if container is None:
                    raise Exception(
                        f'Could not find container: {self.data["container_id"]}'
                    )
                g_task_vars.container.set(container)
                annotations_container = IAnnotations(container)
                container_settings = await annotations_container.async_get(
                    REGISTRY_DATA_KEY
                )
                layers = container_settings.get(ACTIVE_LAYERS_KEY, [])
                for layer in layers:
                    try:
                        alsoProvides(request, import_class(layer))
                    except ModuleNotFoundError:
                        pass
                g_task_vars.registry.set(container_settings)
        return request

    async def __call__(self):
        # Clone request for task
        request = await self.create_request()
        try:
            result = await self.__run(request)
            try:
                # Finish and return result
                await commit()
                request.execute_futures()
            except Exception:
                logger.warning("Error commiting job", exc_info=True)
                raise
            return result
        except ObjectNotFoundException:
            # Tried to run a function on an object that no longer
            # exist in the database
            return None
        except Exception:
            raise
        finally:
            try:
                await abort()
            except Exception:
                logger.warning("Error aborting job", exc_info=True)

    def get_function_to_run(self):
        func = resolve_dotted_name(self.data["func"])
        if ITaskDefinition.providedBy(func):
            func = func.func
        if hasattr(func, "__real_func__"):
            # from decorators
            func = func.__real_func__
        return func

    @property
    def function_name(self):
        """
        """
        func = self.get_function_to_run()
        dotted_name = get_dotted_name(func)
        if func in [_run_object_task, _yield_object_task]:
            # Remove guillotina_amqp.utils part
            dotted_name = dotted_name.lstrip("guillotina_amqp.utils")
            # Get actuall callable that is passed as the first parameter
            # of func
            _callable = self.data.get("args", [""])[0]
            dotted_name += f"/{_callable}"

        return dotted_name

    async def __run(self, request):
        result = None
        task_id = self.data["task_id"]
        dotted_name = self.data["func"]
        logger.info(f"Running task: {task_id}: {dotted_name}")

        # Update status
        await update_task_running(self.state_manager, task_id)

        req_data = self.data["req_data"]
        if "user" in req_data:
            login_user(request, req_data["user"])

        # Parse the function to run
        func = self.get_function_to_run()

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

        task_vars.amqp_job.set(self)
        # Function is an async generator
        if inspect.isasyncgenfunction(func):
            async for status in func(*self.data["args"], **self.data["kwargs"]):
                if not isinstance(status, tuple) or len(status) != 2:
                    logger.debug(f"Job: invalid generator event: {status}")
                    continue

                msg_type, content = status

                if msg_type == MessageType.DEBUG:
                    # DEBUG message: record it in the task's event log
                    date_now = datetime.now().isoformat(" ", "milliseconds")
                    logger.debug(
                        f"Job {task_id}: function data {self.data}, got msg {content}"
                    )

                    # Update the status with new log
                    state = await self.state_manager.get(task_id)
                    state.setdefault("eventlog", [])
                    state["eventlog"].append([date_now, content])
                    await self.state_manager.update(task_id, state)

                elif msg_type == MessageType.RESULT:
                    # RESULT value yielded: accumulate all the
                    # generator results in a list
                    logger.debug(
                        f"Job {task_id}: function data {self.data}, got result {content}"
                    )
                    if result is None:
                        result = [content]
                    elif isinstance(result, list):
                        result.append(content)
                else:
                    # Unknown message: log and continue
                    logger.debug(
                        f"Job {task_id}: invalid generator event code {msg_type}"
                    )
                    continue
        else:
            # Regular coroutine
            result = await func(*self.data["args"], **self.data["kwargs"])
        task_vars.amqp_job.set(None)

        return result
