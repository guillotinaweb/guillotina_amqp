from guillotina import app_settings
from guillotina.utils import resolve_dotted_name
from guillotina import glogging

import aioamqp
import asyncio
import uuid
import os
import json

import aioamqp.exceptions


logger = glogging.getLogger('guillotina_amqp')

beacon_ttl_default = 30  # 30 seconds


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
        connection['beaconsmgr'].stop()
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


async def get_beaconsmgr_for_connection(name='default'):
    # Returns the beacons manager associated with an AMQP connection
    connections = app_settings['amqp']['connections']
    if name in connections:
        return connections[name]['beaconsmgr']


async def get_connection(name='default'):
    amqp_settings = app_settings['amqp']
    if 'connections' not in amqp_settings:
        amqp_settings['connections'] = {}
    connections = amqp_settings['connections']
    if name in connections:
        connection = connections[name]
        return connection['channel'], connection['transport'], connection['protocol']
    channel, transport, protocol = await connect()

    # Create the BeaconsManager and set up the beacon queues
    beacons_mgr = BeaconsManager(channel,
                                 ttl=amqp_settings.get('beaconttl', 30))
    await beacons_mgr.setup_beacon_queues()

    connections[name] = {
        'channel': channel,
        'protocol': protocol,
        'transport': transport,
        'beaconsmgr': beacons_mgr
    }
    asyncio.ensure_future(handle_connection_closed(name, protocol))
    if amqp_settings.get('heartbeat_task') is None:
        logger.info('Starting amqp heartbeat')
        amqp_settings.update({
            'heartbeat_task': asyncio.ensure_future(heartbeat())
        })
    return channel, transport, protocol


class BeaconsManager:
    def __init__(self, channel, ttl=beacon_ttl_default):
        self._ttl = ttl
        self._stopped = False
        self.channel = channel

        # Autokill task
        self.autokill_handler = None

        # Autokill-ed event
        self.autokill_event = None

        # Beacon UUID and queues names
        self.worker_beacon_uuid = uuid.uuid4().hex
        self.beacon_queue_name = f'beacon-{self.worker_beacon_uuid}'
        self.beacon_delay_queue_name = f'beacon-delay-{self.worker_beacon_uuid}'

    @property
    def ttl(self):
        # Beacon TTL
        return self._ttl

    async def setup_beacon_queues(self):
        self.autokill_event = asyncio.Event()

        logger.debug(f'Setting up beacon queues with TTL {self.ttl}')

        # Declare beacon exchange
        await self.channel.exchange_declare(
            exchange_name='beacon',
            type_name='direct',
        )

        # Declare and bind the main beacon temporary queue
        await self.channel.queue_declare(
            queue_name=self.beacon_queue_name,
            exclusive=True,  # This deletes the queue if connection is lost
        )
        await self.channel.queue_bind(
            exchange_name='beacon',
            queue_name=self.beacon_queue_name,
            routing_key=self.beacon_queue_name,
        )

        # Declare and bind delay beacon temporary queue
        await self.channel.queue_declare(
            queue_name=self.beacon_delay_queue_name,
            exclusive=True,  # This deletes the queue if connection is lost
            arguments={
                'x-dead-letter-exchange': 'beacon',
                'x-dead-letter-routing-key': self.beacon_queue_name,
                'x-message-ttl': self.ttl,  # 30 seconds
            })

        await self.channel.queue_bind(
            exchange_name='beacon',
            queue_name=self.beacon_delay_queue_name,
            routing_key=self.beacon_delay_queue_name,
        )

        # Configure main beacon message handler
        await self.channel.basic_consume(
            self.handle_beacon,
            queue_name=self.beacon_queue_name,
        )

        # Start the autokill task
        self.autokill_handler = asyncio.ensure_future(self.autokill(self.ttl))

        # Send the first beacon
        asyncio.ensure_future(self.publish_beacon_to_delay_queue(wait=self.ttl))

    async def autokill(self, ttl):
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

        self.autokill_event.set()
        loop.call_later(2, os._exit, 0)

    async def publish_beacon_to_delay_queue(self, wait=0):
        # Publish to beacon delay queue
        await asyncio.sleep(wait)
        try:
            beacon_payload = json.dumps({'worker_beacon_uuid': self.worker_beacon_uuid})
            await self.channel.publish(
                beacon_payload,
                exchange_name='beacon',
                routing_key=self.beacon_delay_queue_name,
                properties={
                    'delivery_mode': 2,
                }
            )
        except aioamqp.exceptions.ChannelClosed:
            logger.warning(f'Beacon publish: channel is closed !')
        else:
            logger.info(f'Beacon published: {beacon_payload}')

    async def handle_beacon(self, channel, body, envelope, properties):
        logger.debug(f'handle_beacon {body} {channel}')

        if not self._stopped and self.autokill_handler:
            # ACK beacon queue
            await self.channel.basic_client_ack(
                delivery_tag=envelope.delivery_tag,
            )
            # Cancel previous autokill
            self.autokill_handler.cancel()

        # Schedule sending of beacon
        if not self._stopped:
            await self.publish_beacon_to_delay_queue(wait=self.ttl)

    def stop(self):
        self._stopped = True
        if self.autokill_handler:
            self.autokill_handler.cancel()
            self.autokill_handler = None


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
    return channel, transport, protocol
