from guillotina import configure
from .state import get_state_manager


@configure.service(method='GET', name='@amqp-tasks',
                   permission='guillotina.AccessContent')
async def list_tasks(context, request):
    mngr = get_state_manager()
    return await mngr.list()


@configure.service(method='GET', name='@amqp-info',
                   permission='guillotina.AccessContent')
async def info_tasks(context, request):
    mngr = get_state_manager()
    return await mngr.list()
