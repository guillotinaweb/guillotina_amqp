from guillotina import app_settings
from guillotina.utils import resolve_dotted_name

import aioamqp
import asyncio
import logging
import uuid
import os
import json

import aioamqp.exceptions


logger = logging.getLogger('guillotina_amqp')

autokill_handler = None
autokill_event = None
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


async def setup_beacon_queues(channel, ttl=beacon_ttl):
    global beacon_queue_name
    global beacon_delay_queue_name
    global autokill_handler
    global autokill_event

    autokill_event = asyncio.Event()

    logger.debug(f'Setting up beacon queues with TTL {ttl}')

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
            'x-message-ttl': ttl,  # 30 seconds
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

    # Start the autokill task
    autokill_handler = asyncio.ensure_future(autokill(ttl))

    # Send the first beacon
    asyncio.ensure_future(publish_beacon_to_delay_queue(channel, ttl))


async def autokill(ttl):
    """
    AutoKill task, sleeps as long as beacons are received, and exits the
    process otherwise

    This task captures cancel events (cancel is called from the beacon handler)
    and keeps sleeping in that case, otherwise exits the process because that
    means we didn't receive any beacons (AMQP connection is stalled)
    """
    loop = asyncio.get_event_loop()

    while True:
        try:
            asleep = ttl * 3
            logger.debug(f'autokill: sleeping for {asleep} seconds')
            await asyncio.sleep(asleep)
        except asyncio.CancelledError:
            logger.debug(f'autokill: received cancel, beacon was received')
            continue
        except Exception as err:
            logger.debug(f'autokill: unknown exception {err}')
            continue
        else:
            logger.debug(f'autokill: No beacons received')
            break

    # No beacons received: set the autokill event, and schedule a call
    # to os._exit a bit later so that unit tests can wait on autokill_event

    logger.error(f'Exiting worker because of no beacon activity')

    autokill_event.set()
    loop.call_later(2, os._exit, 0)


async def publish_beacon_to_delay_queue(channel, wait=0):
    # Publish to beacon delay queue
    await asyncio.sleep(wait)
    try:
        beacon_payload = json.dumps({'worker_beacon_uuid': worker_beacon_uuid})
        await channel.publish(
            beacon_payload,
            exchange_name='beacon',
            routing_key=beacon_delay_queue_name,
            properties={
                'delivery_mode': 2,
            }
        )
    except aioamqp.exceptions.ChannelClosed:
        logger.debug(f'Beacon publish: channel is closed !')
    else:
        logger.debug(f'Beacon published: {beacon_payload}')


async def handle_beacon(channel, body, envelope, properties):
    global autokill_handler
    global worker_beacon_uuid
    global beacon_delay_queue_name

    logger.debug(f'handle_beacon {body} {channel}')

    if autokill_handler:
        # ACK beacon queue
        await channel.basic_client_ack(
            delivery_tag=envelope.delivery_tag,
        )
        # Cancel previous autokill
        autokill_handler.cancel()

    # Schedule sending of beacon
    ttl = app_settings['amqp'].get('beaconttl', None)
    if ttl:
        await publish_beacon_to_delay_queue(channel, wait=ttl)


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
    await setup_beacon_queues(
        channel, ttl=amqp_settings.get('beaconttl', 30))

    return channel, transport, protocol
