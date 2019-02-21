from guillotina import configure
from guillotina.interfaces import IContainer
from aiohttp.web_exceptions import HTTPNotFound
from .state import TaskState
from .state import get_state_manager
from .exceptions import TaskNotFoundException


@configure.service(
    context=IContainer,
    method='GET', name='@amqp-tasks',
    permission='guillotina.ManageAMQP',
    summary='Returns the list of running tasks')
async def list_tasks(context, request):
    mngr = get_state_manager()
    ret = []
    async for el in mngr.list():
        ret.append(el)
    return ret


@configure.service(
    context=IContainer,
    method='GET', name='@amqp-info/{task_id}',
    permission='guillotina.ManageAMQP',
    summary='Shows the info of a given task id')
async def info_task(context, request):
    try:
        task = TaskState(request.matchdict['task_id'])
        return await task.get_state()
    except TaskNotFoundException:
        raise HTTPNotFound(reason='Task not found')


@configure.service(
    context=IContainer,
    method='DELETE', name='@amqp-cancel/{task_id}',
    permission='guillotina.ManageAMQP',
    summary='Cancel a specific task by id')
async def cancel_task(context, request):
    task = TaskState(request.matchdict['task_id'])
    try:
        return await task.cancel()
    except TaskNotFoundException:
        raise HTTPNotFound(reason='Task not found')
