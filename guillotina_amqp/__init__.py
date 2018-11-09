from .decorators import object_task  # noqa
from .decorators import task  # noqa
from .utils import add_object_task  # noqa
from .utils import add_task  # noqa
from .api import *  # noqa

app_settings = {
    "amqp": {
        "connection_factory": "aioamqp.connect",
        "host": "localhost",
        "port": 5673,
        "login": "guest",
        "password": "guest",
        "vhost": "/",
        "heartbeat": 800,
        "exchange": "",
        "queue": "guillotina",
        "persistent_manager": "memory"
    },
    'commands': {
        "amqp-worker": "guillotina_amqp.commands.worker.WorkerCommand"
    }
}


def includeme(root):
    """
    custom application initialization here
    """
    pass
