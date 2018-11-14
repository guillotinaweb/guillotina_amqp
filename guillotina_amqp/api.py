from guillotina import configure

from guillotina.response import HTTPPreconditionFailed
from guillotina.response import HTTPNotFound

from .state import get_state_manager


@configure.service(method='GET', name='@amqp-tasks',
                   permission='guillotina.AccessContent',
                   summary='Returns the list of running tasks')
async def list_tasks(context, request):
    mngr = get_state_manager()
    ret = []
    async for el in mngr.list():
        ret.append(el)
    return ret


@configure.service(
    method='GET', name='@amqp-info',
    permission='guillotina.AccessContent',
    summary='Shows the info of a given task id',
    parameters=[{
        "name": "task_id",
        "in": "query",
        "required": True,
        "type": "string",
    }])
async def info_task(context, request):
    mngr = get_state_manager()

    task_id = request.rel_url.query.get('task_id')
    if not task_id:
        raise HTTPPreconditionFailed(content={
            'reason': 'Missing task_id'
        })

    data = await mngr.get(task_id)
    if not data:
        raise HTTPNotFound(content={
            'reason': 'Task not found'
        })

    return await mngr.get(task_id)


@configure.service(
    method='DELETE', name='@amqp-cancel',
    permission='guillotina.AccessContent',
    summary='Cancel a specific task by id',
    parameters=[{
        "name": "task_id",
        "in": "query",
        "required": True,
        "type": "string",
    }])
async def cancel_task(context, request):
    mngr = get_state_manager()

    task_id = request.rel_url.query.get('task_id')
    if not task_id:
        raise HTTPPreconditionFailed(content={
            'reason': 'Missing task_id'
        })

    if await mngr.is_canceled(task_id):
        return {
            'ok': True,
            'info': 'already canceled'
        }

    if not await mngr.exists(task_id):
        raise HTTPNotFound(content={
            'reason': 'Task not found'
        })

    if await mngr.cancel(task_id):
        return {
            'ok': True,
            'info': 'task canceled'
        }

    return {
        'ok': False,
        'info': 'could not cancel task'
    }


"""
@configure.service(method='GET', name='@amqp-stats',
                   permission='guillotina.AccessContent')
async def task_stats(context, request):
    mngr = get_state_manager()
    return await mngr.list()
"""
