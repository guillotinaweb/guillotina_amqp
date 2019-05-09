from .exceptions import TaskNotFoundException
from .state import get_state_manager
from .state import TaskState
from guillotina import configure
from guillotina.interfaces import IContainer
from guillotina.response import HTTPNotFound


@configure.service(method='GET', name='@amqp-tasks', context=IContainer,
                   permission='guillotina.ManageAMQP',
                   summary='Returns the list of running tasks')
async def list_tasks(context, request):
    mngr = get_state_manager()
    ret = []
    async for el in mngr.list():
        ret.append(el)
    return ret


@configure.service(
    method='GET', name='@amqp-tasks/{task_id}', context=IContainer,
    permission='guillotina.ManageAMQP',
    summary='Shows the info of a given task id')
async def info_task(context, request):
    try:
        task = TaskState(request.matchdict['task_id'])
        state = await task.get_state()
        if state.get('job_data', {}).get('container_id') == context.id:
            return state
    except TaskNotFoundException:
        raise HTTPNotFound(content={
            'reason': 'Task not found'
        })


@configure.service(
    method='DELETE', name='@amqp-tasks/{task_id}', context=IContainer,
    permission='guillotina.ManageAMQP',
    summary='Cancel a specific task by id')
async def cancel_task(context, request):
    task = TaskState(request.matchdict['task_id'])
    try:
        state = await task.get_state()
        if state.get('job_data', {}).get('container_id') == context.id:
            return await task.cancel()
    except TaskNotFoundException:
        pass

    raise HTTPNotFound(content={
        'reason': 'Task not found'
    })
