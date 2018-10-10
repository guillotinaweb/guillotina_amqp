from guillotina import configure
from .state import get_state_manager
from .exceptions import TaskNotFoundException


@configure.service(method='GET', name='@amqp-tasks',
                   permission='guillotina.AccessContent')
async def list_tasks(context, request):
    mngr = get_state_manager()

    ret = []
    async for el in mngr.list():
        ret.append(el)
    return ret


@configure.service(method='GET', name='@amqp-info',
                   permission='guillotina.AccessContent')
async def info_task(context, request):
    mngr = get_state_manager()
    task_id = request.rel_url.query.get('task_id')
    if not task_id:
        raise TaskNotFinishedException()

    return await mngr.get(task_id)


@configure.service(method='DELETE', name='@amqp-cancel',
                   permission='guillotina.AccessContent')
async def cancel_task(context, request):
    mngr = get_state_manager()
    task_id = request.rel_url.query.get('task_id')

    return await mngr.list()


@configure.service(method='GET', name='@amqp-stats',
                   permission='guillotina.AccessContent')
async def task_stats(context, request):
    mngr = get_state_manager()
    return await mngr.list()
