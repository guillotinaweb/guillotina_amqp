from guillotina import app_settings
from guillotina import testing
from guillotina_amqp import amqp
from guillotina_amqp.worker import Worker
from pytest_docker_fixtures.containers.rabbitmq import rabbitmq_image

import os
import pytest
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
    "max_running_tasks": 10,
    "delayed_ttl_ms": 1000 * 60,  # <-- 1 minute
    "errored_ttl_ms": 1000 * 60 * 60 * 24 * 7,  # <-- 1 week
    "state_ttl": 10,
}

test_logger_settings = {
    "version": 1,
    "formatters": {
        "default": {
            "format": "%(asctime)s %(levelname)-8s %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "default",
            "level": "DEBUG",
        }
    },
    "loggers": {
        "guillotina_amqp.state": {"level": "DEBUG", "handlers": ["console"]},
        "guillotina_amqp.job": {"level": "DEBUG", "handlers": ["console"]},
        "guillotina_amqp.amqp": {"level": "DEBUG", "handlers": ["console"]},
    },
}


def base_settings_configurator(settings):
    if "applications" in settings:
        settings["applications"].extend(
            [
                "guillotina_amqp",
                "guillotina_amqp.tests.package",
                "guillotina.contrib.redis",
            ]
        )
    else:
        settings["applications"] = [
            "guillotina_amqp",
            "guillotina_amqp.tests.package",
            "guillotina.contrib.redis",
        ]
    settings["amqp"] = base_amqp_settings
    settings["logging"] = test_logger_settings


testing.configure_with(base_settings_configurator)


@pytest.fixture("function")
def amqp_worker(loop):
    # Create worker
    _worker = Worker(loop=loop, check_activity=False)
    _worker.update_status_interval = 2
    loop.run_until_complete(_worker.start())

    yield _worker

    # Tear down worker
    for conn in [v for v in app_settings["amqp"].get("connections", []).values()]:
        loop.run_until_complete(conn["protocol"].close())
    _worker.cancel()
    app_settings["amqp"]["connections"] = {}


@pytest.fixture("function", params=[{"redis_up": True}, {"redis_up": False}])
def configured_state_manager(request, redis, dummy_request, loop):
    if request.param.get("redis_up"):
        # Redis
        app_settings["amqp"]["persistent_manager"] = "redis"
        app_settings["redis_prefix_key"] = f"amqpjobs-{uuid.uuid4()}-"
        app_settings.update(
            {
                "redis": {
                    "host": redis[0],
                    "port": redis[1],
                    "pool": {"minsize": 1, "maxsize": 5},
                }
            }
        )
        print("Running with redis")
        yield redis
    else:
        # Memory
        app_settings["amqp"]["persistent_manager"] = "memory"
        print("Running with memory")
        yield


@pytest.fixture("function")
def redis_state_manager(redis, dummy_request, loop):
    # Redis
    app_settings["amqp"]["persistent_manager"] = "redis"
    app_settings["redis_prefix_key"] = f"amqpjobs-{uuid.uuid4()}-"
    app_settings.update(
        {
            "redis": {
                "host": redis[0],
                "port": redis[1],
                "pool": {"minsize": 1, "maxsize": 5},
            }
        }
    )
    print("Running with redis")
    yield redis


@pytest.fixture("function")
async def amqp_channel():
    channel, transport, protocol = await amqp.get_connection()
    return channel


IS_TRAVIS = "TRAVIS" in os.environ


@pytest.fixture(scope="session")
def rabbitmq_runner():
    if IS_TRAVIS:
        host = "127.0.0.1"
        port = 5672
    else:
        host, port = rabbitmq_image.run()
    yield host, port
    if not IS_TRAVIS:
        rabbitmq_image.stop()


@pytest.fixture("function")
def rabbitmq_container(rabbitmq):
    app_settings["amqp"].update(
        {
            "connection_factory": "aioamqp.connect",
            "host": rabbitmq[0],
            "port": rabbitmq[1],
            "login": "guest",
            "password": "guest",
            "vhost": "/",
            "heartbeat": 120,  # 2 minutes
            "exchange": "guillotina",
            "queue": "guillotina",
        }
    )
