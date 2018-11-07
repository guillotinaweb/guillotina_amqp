from guillotina import testing
import pytest
from guillotina_amqp.worker import Worker
from guillotina import app_settings


def base_settings_configurator(settings):
    if 'applications' in settings:
        settings['applications'].extend([
            'guillotina_amqp', 'guillotina_amqp.tests.package'
        ])
    else:
        settings['applications'] = ['guillotina_amqp', 'guillotina_amqp.tests.package']
    settings['amqp'] = {
        "connection_factory": "guillotina_amqp.tests.mocks.amqp_connection_factory",
        "host": "localhost",
        "port": 5673,
        "login": "guest",
        "password": "guest",
        "vhost": "/",
        "heartbeat": 800,
        "exchange": "",
        "queue": "guillotina",
        "persistent_manager": "memory"
    }


testing.configure_with(base_settings_configurator)


@pytest.fixture('function')
def amqp_worker(loop):
    # Create worker
    _worker = Worker(loop=loop)
    loop.run_until_complete(_worker.start())

    yield _worker

    # Tear down worker
    for conn in [v for v in app_settings['amqp'].get('connections', []).values()]:
        loop.run_until_complete(conn['protocol'].close())
    _worker.cancel()
    app_settings['amqp']['connections'] = {}


@pytest.fixture('function')
def redis_enabled(redis, dummy_request):
    app_settings['amqp']['persistent_manager'] = 'redis'
    app_settings['redis_prefix_key'] = 'amqpjobs-'
    app_settings.update({"redis":{
        'host': redis[0],
        'port': redis[1],
        'pool': {
            "minsize": 1,
            "maxsize": 5,
        },
    }})
    yield redis
