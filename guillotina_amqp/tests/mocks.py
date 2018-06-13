import asyncio
import uuid


class MockChannel:

    def __init__(self):
        self.published = []
        self.acked = []
        self.nacked = []

    async def publish(self, *args, **kwargs):
        self.published.append({
            'args': args,
            'kwargs': kwargs
        })

    async def basic_client_ack(self, *args, **kwargs):
        self.acked.append({
            'args': args,
            'kwargs': kwargs
        })

    async def basic_client_nack(self, *args, **kwargs):
        self.nacked.append({
            'args': args,
            'kwargs': kwargs
        })


class MockEnvelope:

    def __init__(self, uid):
        self.delivery_tag = uid


class MockAMQPChannel:

    def __init__(self, protocol):
        self.protocol = protocol
        self.consumers = []
        self.closed = False
        self.unacked_messages = []

    async def basic_qos(self, *args, **kwargs):
        pass

    async def exchange_declare(self, *args, **kwargs):
        pass

    async def queue_declare(self, queue_name, *args, **kwargs):
        if queue_name not in self.protocol.queues:
            self.protocol.queues[queue_name] = []
        if 'arguments' in kwargs:
            arguments = kwargs['arguments']
            if 'x-dead-letter-routing-key' in arguments:
                self.protocol.dead_mapping[queue_name] = arguments['x-dead-letter-routing-key']

    async def queue_bind(self, *args, **kwargs):
        pass

    async def _basic_consume(self, handler, queue_name):
        while not self.closed:
            await asyncio.sleep(0.02)
            if queue_name not in self.protocol.queues:
                continue
            else:
                messages = self.protocol.queues[queue_name]
                self.protocol.queues[queue_name] = []
                self.unacked_messages.extend(messages)
                for message in messages:
                    await handler(self, message['message'],
                                  MockEnvelope(message['id']), message['properties'])

    async def basic_client_ack(self, delivery_tag):
        for message in self.unacked_messages[:]:
            if delivery_tag == message['id']:
                self.unacked_messages.remove(message)
                return message

    async def basic_client_nack(self, delivery_tag, multiple=False, requeue=False):
        message = await self.basic_client_ack(delivery_tag)
        if message:
            if requeue:
                # put back on same queue
                self.protocol.queues[message['queue']].append(message)
            else:
                new_queue = self.protocol.dead_mapping[message['queue']]
                self.protocol.queues[new_queue].append(message)

    async def basic_consume(self, handler, queue_name):
        self.consumers.append(asyncio.ensure_future(self._basic_consume(handler, queue_name)))

    async def publish(self, message, exchange_name=None, routing_key=None, properties={}):
        if routing_key not in self.protocol.queues:
            self.protocol.queues[routing_key] = []
        self.protocol.queues[routing_key].append({
            'id': str(uuid.uuid4()),
            'message': message,
            'properties': properties,
            'queue': routing_key
        })

    async def close(self):
        self.closed = True
        await asyncio.sleep(0.06)


class MockAMQPTransport:

    def __init__(self):
        pass

    def close(self):
        pass


class MockAMQPProtocol:

    def __init__(self):
        self.queues = {}
        self.dead_mapping = {}
        self.closed = False
        self.channels = []

    async def channel(self):
        channel = MockAMQPChannel(self)
        self.channels.append(channel)
        return channel

    async def wait_closed(self):
        while not self.closed:
            await asyncio.sleep(0.05)
        raise GeneratorExit()

    async def close(self):
        self.closed = True
        for channel in self.channels:
            await channel.close()

    async def send_heartbeat(self):
        pass


async def amqp_connection_factory(*args, **kwargs):
    return MockAMQPTransport(), MockAMQPProtocol()
