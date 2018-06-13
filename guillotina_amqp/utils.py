from guillotina import app_settings
from guillotina.component import get_utility
from guillotina.interfaces import IAbsoluteURL
from guillotina.utils import get_content_path
from guillotina.utils import get_current_request
from guillotina.utils import get_dotted_name
from guillotina.utils import navigate_to
from guillotina.utils import resolve_dotted_name
from guillotina_amqp import amqp
from guillotina_amqp.interfaces import IStateManagerUtility
from guillotina_amqp.state import TaskState

import aioamqp
import json
import logging
import uuid


logger = logging.getLogger('guillotina_amqp')


async def add_task(func, *args, _request=None, _retries=3, **kwargs):
    if _request is None:
        _request = get_current_request()

    req_data = {
        'url': str(_request.url),
        'headers': dict(_request.headers),
        'method': _request.method
    }
    try:
        participation = _request.security.participations[0]
        user = participation.principal
        req_data['user'] = {
            'id': user.id,
            'roles': user.roles,
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
        channel, transport, protocol = await amqp.get_connection()
        try:
            task_id = str(uuid.uuid4())
            state = TaskState(task_id)
            dotted_name = get_dotted_name(func)
            logger.info(f'Scheduling task: {task_id}: {dotted_name}')
            await channel.publish(
                json.dumps({
                    'func': dotted_name,
                    'args': args,
                    'kwargs': kwargs,
                    'db_id': getattr(_request, '_db_id', None),
                    'container_id': getattr(_request, '_container_id', None),
                    'req_data': req_data,
                    'task_id': task_id
                }),
                exchange_name=app_settings['amqp']['exchange'],
                routing_key=app_settings['amqp']['queue'],
                properties={
                    'delivery_mode': 2
                }
            )
            state_manager = get_utility(
                IStateManagerUtility,
                name=app_settings['amqp'].get('persistent_manager', 'dummy'))
            await state_manager.update(task_id, {
                'status': 'scheduled'
            })
            logger.info(f'Scheduled task: {task_id}: {dotted_name}')
            return state
        except (aioamqp.AmqpClosedConnection, aioamqp.exceptions.ChannelClosed):
            await amqp.remove_connection()
            if retries >= _retries:
                raise
            retries += 1


async def _run_object_tasks(dotted_func, path, *args, **kwargs):
    request = get_current_request()
    ob = await navigate_to(request.container, path)
    func = resolve_dotted_name(dotted_func)
    return await func(ob, *args, **kwargs)


async def add_object_task(func, ob, *args, _request=None, _retries=3, **kwargs):
    return await add_task(
        _run_object_tasks, get_dotted_name(func), get_content_path(ob), *args,
        _request=_request, _retries=_retries, **kwargs)
