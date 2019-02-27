from .decorators import object_task  # noqa
from .decorators import task  # noqa
from .utils import add_object_task  # noqa
from .utils import add_task  # noqa
from guillotina import configure


app_settings = {
    "amqp": {
        "connection_factory": "aioamqp.connect",
        "host": "localhost",
        "port": 5672,
        "login": "guest",
        "password": "guest",
        "vhost": "/",
        "heartbeat": 60 * 2,  # 2 minutes
        "exchange": "guillotina",
        "queue": "guillotina",
        "persistent_manager": "memory",
        "max_running_tasks": 200,
        "state_ttl": 60 * 60 * 24,  # 1 day
    },
    'commands': {
        "amqp-worker": "guillotina_amqp.commands.worker.WorkerCommand"
    }
}


def includeme(root):
    """
    custom application initialization here
    """
    configure.scan('guillotina_amqp.permissions')
    configure.scan('guillotina_amqp.api')
