from .utils import add_task  # noqa
from .decorators import task  # noqa


app_settings = {
    "amqp": {
        "connection_factory": "aioamqp.connect",
        "host": "localhost",
        "port": 5673,
        "login": "guest",
        "password": "guest",
        "vhost": "/",
        "heartbeat": 800,
        "exchange": "guillotina",
        "queue": "guillotina",
        "persistent_manager": "dummy"
    }
}


def includeme(root):
    """
    custom application initialization here
    """
    pass
