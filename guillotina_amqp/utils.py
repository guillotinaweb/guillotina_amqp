from guillotina import app_settings
from guillotina.interfaces import IAbsoluteURL
from guillotina.utils import get_content_path
from guillotina.utils import get_current_request
from guillotina.utils import get_dotted_name
from guillotina.utils import navigate_to
from guillotina_amqp import amqp

import aioamqp
import json


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
            await channel.publish(
                message=json.dumps({
                    'func': get_dotted_name(func),
                    'args': args,
                    'kwargs': kwargs,
                    'db_id': getattr(_request, '_db_id', None),
                    'container_id': getattr(_request, '_container_id', None),
                    'req_data': req_data
                }),
                exchange_name=app_settings['amqp']['exchange'],
                routing_key=app_settings['amqp']['queue'],
                properties={
                    'delivery_mode': 2
                }
            )
            return
        except (aioamqp.AmqpClosedConnection, aioamqp.exceptions.ChannelClosed):
            await amqp.remove_connection()
            if retries >= _retries:
                raise
            retries += 1


async def _run_object_tasks(func, path):
    request = get_current_request()
    ob = await navigate_to(request.container, path)
    return await func(ob)


async def add_object_task(func, ob, _request=None, _retries=3):
    await add_task(
        _run_object_tasks, get_content_path(ob),
        _request=_request, _retries=_retries)
