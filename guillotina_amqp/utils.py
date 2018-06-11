from guillotina import app_settings
from guillotina.utils import get_current_request
from guillotina.utils import get_dotted_name
from guillotina_amqp import amqp
from guillotina.interfaces import IAbsoluteURL

import aioamqp
import json


async def add_task(func, *args, _request=None, _db_txn=True, _retries=3, **kwargs):
    if _request is None:
        _request = get_current_request()

    user_data = {}
    try:
        participation = _request.security.participations[0]
        user = participation.principal
        user_data = {
            'id': user.id,
            'roles': user.roles,
            'groups': user.groups,
            'Authorization': _request.headers.get('Authorization'),
            'data': getattr(user, 'data', {})
        }
    except (AttributeError, IndexError):
        pass

    if getattr(_request, 'container', None):
        user_data['container_url'] = IAbsoluteURL(_request.container, _request)()

    retries = 0
    while True:
        channel, transport, protocol = await amqp.get_connection()
        try:
            await channel.basic_publish(
                payload=json.dumps({
                    'func': get_dotted_name(func),
                    'args': args,
                    'kwargs': kwargs,
                    'db_txn': _db_txn,
                    'db_id': getattr(_request, '_db_id', None),
                    'container_id': getattr(_request, '_container_id', None),
                    'user_data': user_data
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
