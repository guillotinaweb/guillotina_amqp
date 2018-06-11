from .utils import add_task  # noqa


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
        "queue": "guillotina"
    }
}


def includeme(root):
    """
    custom application initialization here
    """
    pass
