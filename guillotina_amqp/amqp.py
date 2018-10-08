from guillotina import app_settings
from guillotina.utils import resolve_dotted_name

import aioamqp
import asyncio
import logging


logger = logging.getLogger('guillotina_amqp')


async def remove_connection(name='default'):
    '''
    Purpose here is to close out a bad connection.
    Next time get_connection is called, a new connection will be established
    '''
    ampq_settings = app_settings['amqp']
    if 'connections' not in ampq_settings:
        ampq_settings['connections'] = {}
    connections = ampq_settings['connections']
    if name not in connections:
        return

    connection = connections.pop(name)
    try:
        await connection['channel'].close()
    except Exception:
        pass  # could already be closed
    try:
        await connection['protocol'].close(no_wait=True)
    except Exception:
        pass  # could already be closed
    try:
        connection['transport'].close()
    except Exception:
        pass


async def handle_connection_closed(name, protocol):
    try:
        # this just waits for the connection to close
        try:
            await protocol.wait_closed()
        except GeneratorExit:
            return
        # we just remove so next time get_connection is retrieved, a new
        # connection is retrieved.
        logger.warn('Disconnect detected with rabbitmq connection, forcing reconnect')
        await remove_connection(name)
    except Exception:
        logger.error('Error waiting for connection to close', exc_info=True)


async def heartbeat():
    while True:
        await asyncio.sleep(20)
        try:
            for name, connection in app_settings['amqp'].get('connections', {}).items():
                await connection['protocol'].send_heartbeat()
        except Exception:
            logger.error('Error sending heartbeat', exc_info=True)


async def get_connection(name='default'):
    ampq_settings = app_settings['amqp']
    if 'connections' not in ampq_settings:
        ampq_settings['connections'] = {}
    connections = ampq_settings['connections']
    if name in connections:
        connection = connections[name]
        return connection['channel'], connection['transport'], connection['protocol']
    channel, transport, protocol = await connect()
    connections[name] = {
        'channel': channel,
        'protocol': protocol,
        'transport': transport
    }
    asyncio.ensure_future(handle_connection_closed(name, protocol))
    if ampq_settings.get('heartbeat_task') is None:
        logger.info('Starting amqp heartbeat')
        ampq_settings.update({
            'heartbeat_task': asyncio.ensure_future(heartbeat())
        })
    return channel, transport, protocol


async def connect(**kwargs):
    ampq_settings = app_settings['amqp']
    conn_factory = resolve_dotted_name(
        ampq_settings.get('connection_factory', aioamqp.connect))
    transport, protocol = await conn_factory(
        host=ampq_settings['host'],
        port=ampq_settings['port'],
        login=ampq_settings['login'],
        password=ampq_settings['password'],
        virtualhost=ampq_settings['vhost'],
        heartbeat=800,
        **kwargs
    )
    channel = await protocol.channel()

    return channel, transport, protocol
