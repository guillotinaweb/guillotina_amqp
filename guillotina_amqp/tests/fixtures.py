from guillotina import testing


def base_settings_configurator(settings):
    if 'applications' in settings:
        settings['applications'].append('guillotina_amqp')
    else:
        settings['applications'] = ['guillotina_amqp']
    settings['amqp'] = {
        "connection_factory": "guillotina_amqp.tests.mocks.amqp_connection_factory",
        "host": "localhost",
        "port": 5673,
        "login": "guest",
        "password": "guest",
        "vhost": "/",
        "heartbeat": 800,
        "exchange": "guillotina",
        "queue": "guillotina"
    }


testing.configure_with(base_settings_configurator)
