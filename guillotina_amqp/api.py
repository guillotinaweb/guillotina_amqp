from .exceptions import TaskNotFoundException
from .state import get_state_manager
from .state import TaskState
from .utils import get_task_id_prefix
from guillotina import configure
from guillotina.interfaces import IContainer
from guillotina.response import HTTPNotFound


@configure.service(method='GET', name='@amqp-tasks', context=IContainer,
                   permission='guillotina.ManageAMQP',
                   summary='Returns the list of running tasks')
async def list_tasks(context, request):
    mngr = get_state_manager()
    ret = []
    task_prefix = get_task_id_prefix(request)
    async for el in mngr.list():
        if not el.startswith(task_prefix):
            ret.append(el)
    return ret


@configure.service(
    method='GET', name='@amqp-tasks/{task_id}', context=IContainer,
    permission='guillotina.ManageAMQP',
    summary='Shows the info of a given task id')
async def info_task(context, request):
    task_prefix = get_task_id_prefix(request)
    if not request.matchdict['task_id'].startswith(task_prefix):
        raise HTTPNotFound(content={
            'reason': 'Task not found'
        })
    try:
        task = TaskState(request.matchdict['task_id'])
        return await task.get_state()
    except TaskNotFoundException:
        raise HTTPNotFound(content={
            'reason': 'Task not found'
        })


@configure.service(
    method='DELETE', name='@amqp-tasks/{task_id}', context=IContainer,
    permission='guillotina.ManageAMQP',
    summary='Cancel a specific task by id')
async def cancel_task(context, request):
    task_prefix = get_task_id_prefix(request)
    if not request.matchdict['task_id'].startswith(task_prefix):
        raise HTTPNotFound(content={
            'reason': 'Task not found'
        })
    task = TaskState(request.matchdict['task_id'])
    try:
        return await task.cancel()
    except TaskNotFoundException:
        raise HTTPNotFound(content={
            'reason': 'Task not found'
        })
