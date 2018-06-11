from guillotina import app_settings
from guillotina.auth.participation import GuillotinaParticipation
from guillotina.auth.users import GuillotinaUser
from guillotina.browser import get_physical_path
from guillotina.commands import Command
from guillotina.security.policy import Interaction
from guillotina_amqp import amqp
from urllib.parse import urlparse

import aiotask_context
import logging
import yarl


logger = logging.getLogger('guillotina_amqp')


def login_user(request, user_data):
    request.security = Interaction(request)
    participation = GuillotinaParticipation(request)
    participation.interaction = None

    if 'id' in user_data:
        user = GuillotinaUser(request)
        user.id = user_data['id']
        user._groups = user_data.get('groups', [])
        user._roles = user_data.get('roles', [])
        user.data = user_data.get('data', {})
        participation.principal = user
        request._cache_user = user

    request.security.add(participation)
    request.security.invalidate_cache()
    request._cache_groups = {}
    if user_data.get('Authorization'):
        request.headers['Authorization'] = user_data['Authorization']


def setup_request(request, user_data):
    if 'container_url' in user_data and getattr(request, '_db_id', None):
        container_url = user_data['container_url']
        parsed_url = urlparse(container_url)
        request._cache.clear()
        if 'https' in container_url:
            request._secure_proxy_ssl_header = ('FORCE_SSL', 'true')
            request.headers['FORCE_SSL'] = 'true'
        request.headers.update({
            'HOST': parsed_url.hostname,
            'X-VirtualHost-Monster': container_url.replace(
                request._db_id + '/'.join(get_physical_path(request.container)), ''
            )
        })
        request._rel_url = yarl.URL(yarl.URL(container_url).path)


class Worker:
    concurrency = 5

    def __init__(self, request):
        self.request = request

    async def handle_queued_job(self, channel, body, envelope, properties):
        login_user(request, user_data)
        setup_request(request, user_data)

    async def __call__(self):
        channel, transport, protocol = await amqp.get_connection()
        await channel.basic_qos(prefetch_count=4)
        await channel.basic_consume(
            self.handle_queued_job,
            queue_name=app_settings['amqp']['queue'])


class WorkerCommand(Command):
    description = 'AMQP worker'

    async def run(self, arguments, settings, app):
        aiotask_context.set('request', self.request)
        worker = Worker(self.request)
        await worker()
