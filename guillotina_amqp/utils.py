from .metrics import watch_amqp
from guillotina import app_settings
from guillotina import glogging
from guillotina import task_vars
from guillotina.interfaces import Allow
from guillotina.interfaces import IAbsoluteURL
from guillotina.utils import get_authenticated_user
from guillotina.utils import get_content_path
from guillotina.utils import get_current_request
from guillotina.utils import get_dotted_name
from guillotina.utils import navigate_to
from guillotina.utils import resolve_dotted_name
from guillotina.utils.misc import get_current_container
from guillotina_amqp import amqp
from guillotina_amqp.exceptions import AMQPConfigurationNotFoundError
from guillotina_amqp.exceptions import ObjectNotFoundException
from guillotina_amqp.interfaces import ITaskDefinition
from guillotina_amqp.state import get_state_manager
from guillotina_amqp.state import TaskState
from guillotina_amqp.state import update_task_scheduled

import aioamqp
import asyncio
import inspect
import json
import time
import uuid


logger = glogging.getLogger("guillotina_amqp.utils")


async def cancel_task(task_id):
    """It cancels a task by id. Returns wether it could be cancelled.

    """
    task = TaskState(task_id)
    success = await task.cancel()
    return success


def get_task_id_prefix():
    db = task_vars.db.get()
    container = task_vars.container.get()
    return "task:{}-{}-".format(db.id, container.id)


def generate_task_id():
    container = task_vars.container.get()
    if container is not None:
        return "{}{}".format(get_task_id_prefix(), str(uuid.uuid4()))
    return str(uuid.uuid4())


async def add_task(func, *args, _request=None, _retries=3, _task_id=None, **kwargs):
    """Given a function and its arguments, it adds it as a task to be ran
    by workers.
    """
    # Get the request and prepare request data
    if _request is None:
        _request = get_current_request()

    req_data = {
        "url": str(_request.url),
        "headers": dict(_request.headers),
        "method": _request.method,
        "annotations": getattr(_request, "annotations", {}),
    }
    user = get_authenticated_user()
    if user is not None:
        try:
            req_data["user"] = {
                "id": user.id,
                "roles": [
                    name for name, setting in user.roles.items() if setting == Allow
                ],
                "groups": user.groups,
                "headers": dict(_request.headers),
                "data": getattr(user, "data", {}),
            }
        except AttributeError:
            pass

    container = task_vars.container.get()
    if container is not None:
        req_data["container_url"] = IAbsoluteURL(container, _request)()

    if _task_id is None:
        task_id = generate_task_id()
    else:
        task_id = _task_id

    dotted_name = get_dotted_name(func)

    retries = 0
    while True:
        # Get the rabbitmq connection
        try:
            channel, transport, protocol = await amqp.get_connection()
        except AMQPConfigurationNotFoundError:
            logger.warning(
                f"Could not schedule {dotted_name}, AMQP settings not configured"
            )
            return
        try:
            state = TaskState(task_id)
            db = task_vars.db.get()
            logger.info(f"Scheduling task: {task_id}: {dotted_name}")
            data = json.dumps(
                {
                    "func": dotted_name,
                    "args": args,
                    "kwargs": kwargs,
                    "db_id": getattr(db, "id", None),
                    "container_id": getattr(container, "id", None),
                    "req_data": req_data,
                    "task_id": task_id,
                }
            )
            # Publish task data on rabbitmq
            with watch_amqp("publish"):
                await channel.publish(
                    data,
                    exchange_name=app_settings["amqp"]["exchange"],
                    routing_key=app_settings["amqp"]["queue"],
                    properties={"delivery_mode": 2},
                )
            # Update tasks's global state
            state_manager = get_state_manager()
            await update_task_scheduled(state_manager, task_id, updated=time.time())
            logger.info(f"Scheduled task: {task_id}: {dotted_name}")
            return state
        except (aioamqp.AmqpClosedConnection, aioamqp.exceptions.ChannelClosed):
            await amqp.remove_connection()
            if retries >= _retries:
                raise
            retries += 1


async def _prepare_func(dotted_func, path, *args, **kwargs):
    container = get_current_container()
    try:
        ob = await navigate_to(container, path)
    except KeyError:
        logger.warning(f"Object in {path} not found")
        raise ObjectNotFoundException
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


async def add_object_task(
    callable=None, ob=None, *args, _request=None, _retries=3, **kwargs
):
    superfunc = _run_object_task
    if inspect.isasyncgenfunction(callable):
        # async generators need to be yielded from
        superfunc = _yield_object_task
    return await add_task(
        superfunc,
        get_dotted_name(callable),
        get_content_path(ob),
        *args,
        _request=_request,
        _retries=_retries,
        **kwargs,
    )


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
