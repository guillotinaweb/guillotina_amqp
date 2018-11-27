from guillotina import testing
import pytest
from guillotina_amqp.worker import Worker
from guillotina_amqp import amqp
from guillotina import app_settings
import uuid

base_amqp_settings = {
    "connection_factory": "guillotina_amqp.tests.mocks.amqp_connection_factory",
    "host": "localhost",
    "port": 5673,
    "login": "guest",
    "password": "guest",
    "vhost": "/",
    "heartbeat": 120,  # 2 minutes
    "exchange": "",
    "queue": "guillotina",
    "persistent_manager": "memory",
}


def base_settings_configurator(settings):
    if 'applications' in settings:
        settings['applications'].extend([
            'guillotina_amqp', 'guillotina_amqp.tests.package'
        ])
    else:
        settings['applications'] = ['guillotina_amqp', 'guillotina_amqp.tests.package']
    settings['amqp'] = base_amqp_settings


testing.configure_with(base_settings_configurator)


@pytest.fixture('function')
def amqp_worker(loop, rabbitmq_container):
    amqp.logger.setLevel(10)

    # Create worker
    _worker = Worker(loop=loop)
    _worker.update_status_interval = 2
    loop.run_until_complete(_worker.start())

    yield _worker

    # Tear down worker
    for conn in [v for v in app_settings['amqp'].get('connections', []).values()]:
        loop.run_until_complete(conn['protocol'].close())
    _worker.cancel()
    app_settings['amqp']['connections'] = {}


@pytest.fixture('function', params=[
    {'redis_up': True},
    {'redis_up': False},
])
def configured_state_manager(request, redis, dummy_request, loop):
    if request.param.get('redis_up'):
        # Redis
        app_settings['amqp']['persistent_manager'] = 'redis'
        app_settings['redis_prefix_key'] = f'amqpjobs-{uuid.uuid4()}-'
        app_settings.update({"redis": {
            'host': redis[0],
            'port': redis[1],
            'pool': {
                "minsize": 1,
                "maxsize": 5,
            },
        }})
        print('Running with redis')
        yield redis

        # NOTE: we need to close the redis pool otherwise it's
        # attached to the first loop and the nexts tests have new
        # loops, which causes its to crash
        from guillotina_rediscache.cache import close_redis_pool
        loop.run_until_complete(close_redis_pool())
    else:
        # Memory
        app_settings['amqp']['persistent_manager'] = 'memory'
        print('Running with memory')
        yield


@pytest.fixture('function')
async def amqp_channel():
    channel, transport, protocol = await amqp.get_connection()
    return channel


@pytest.fixture('function')
def rabbitmq_container(rabbitmq):
    app_settings['amqp'].update({
        "connection_factory": "aioamqp.connect",
        "host": rabbitmq[0],
        "port": rabbitmq[1],
        "login": "guest",
        "password": "guest",
        "vhost": "/",
        "heartbeat": 120,  # 2 minutes
        "exchange": "guillotina",
        "queue": "guillotina",
        "beaconttl": 2
    })
