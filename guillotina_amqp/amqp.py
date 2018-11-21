from guillotina import app_settings
from guillotina.utils import resolve_dotted_name

import aioamqp
import asyncio
import logging
import uuid
import os
import json


logger = logging.getLogger('guillotina_amqp')

autokill_handler = None
worker_beacon_uuid = uuid.uuid4().hex
beacon_queue_name = f'beacon-{worker_beacon_uuid}'
beacon_delay_queue_name = f'beacon-delay-{worker_beacon_uuid}'
beacon_ttl = 30  # 30 seconds


async def remove_connection(name='default'):
    '''
    Purpose here is to close out a bad connection.
    Next time get_connection is called, a new connection will be established
    '''
    amqp_settings = app_settings['amqp']
    if 'connections' not in amqp_settings:
        amqp_settings['connections'] = {}
    connections = amqp_settings['connections']
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
        logger.warning('Disconnect detected with rabbitmq connection, forcing reconnect')
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
    amqp_settings = app_settings['amqp']
    if 'connections' not in amqp_settings:
        amqp_settings['connections'] = {}
    connections = amqp_settings['connections']
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
    if amqp_settings.get('heartbeat_task') is None:
        logger.info('Starting amqp heartbeat')
        amqp_settings.update({
            'heartbeat_task': asyncio.ensure_future(heartbeat())
        })
    return channel, transport, protocol


async def setup_beacon_queues(channel):
    global beacon_queue_name
    global beacon_delay_queue_name

    # Declare beacon exchange
    await channel.exchange_declare(
        exchange_name='beacon',
        type_name='direct',
    )

    # Declare and bind the main beacon temporary queue
    await channel.queue_declare(
        queue_name=beacon_queue_name,
        exclusive=True,  # This deletes the queue if connection is lost
    )
    await channel.queue_bind(
        exchange_name='beacon',
        queue_name=beacon_queue_name,
        routing_key=beacon_queue_name,
    )

    # Declare and bind delay beacon temporary queue
    await channel.queue_declare(
        queue_name=beacon_delay_queue_name,
        exclusive=True,  # This deletes the queue if connection is lost
        arguments={
            'x-dead-letter-exchange': 'beacon',
            'x-dead-letter-routing-key': beacon_queue_name,
            'x-message-ttl': 30,  # 30 seconds
        })
    await channel.queue_bind(
        exchange_name='beacon',
        queue_name=beacon_delay_queue_name,
        routing_key=beacon_delay_queue_name,
    )

    # Configure main beacon message handler
    await channel.basic_consume(
        handle_beacon,
        queue_name=beacon_queue_name,
    )


async def autokill():
    global beacon_ttl
    await asyncio.sleep(beacon_ttl * 3)
    logger.error(f'Exiting worker because of no beacon activity')
    os._exit(1)


async def handle_beacon(channel, body, envelope, properties):
    global autokill_handler
    global worker_beacon_uuid
    global beacon_delay_queue_name

    data = json.loads(body)
    if data.get('worker_beacon_uuid') != worker_beacon_uuid:
        # Ignore beacon
        return

    if autokill_handler:
        # ACK beacon queue
        await channel.basic_client_ack(
            delivery_tag=envelope.delivery_tag,
        )
        # Cancel previous autokill
        autokill_handler.cancel()

    # Re-schedule autokill_handler
    autokill_handler = asyncio.ensure_future(autokill())

    # Publish to beacon delay queue
    await channel.publish(
        json.dumps({'worker_beacon_uuid': worker_beacon_uuid}),
        exchange_name='beacon',
        routing_key=beacon_delay_queue_name,
        properties={
            'delivery_mode': 2,
        }
    )


async def connect(**kwargs):
    amqp_settings = app_settings['amqp']
    conn_factory = resolve_dotted_name(
        amqp_settings.get('connection_factory', aioamqp.connect))
    transport, protocol = await conn_factory(
        amqp_settings['host'],
        amqp_settings['port'],
        amqp_settings['login'],
        amqp_settings['password'],
        amqp_settings['vhost'],
        heartbeat=amqp_settings['heartbeat'],
        **kwargs
    )
    channel = await protocol.channel()

    # Setup beacon liveness queues
    await setup_beacon_queues(channel)

    return channel, transport, protocol
