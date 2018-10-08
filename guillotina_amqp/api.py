from guillotina import configure
from guillotina.interfaces import IApplication
from guillotina.response import HTTPNotFound
from guillotina_amqp.exceptions import TaskNotFoundException
from guillotina_amqp.state import TaskState


@configure.service(
    context=IApplication, method='GET', permission='guillotina.Manage',
    name='@task-status/{id}')
async def get_task_data(context, request):
    try:
        task = TaskState(request.matchdict['id'])
        return await task.get_state()
    except TaskNotFoundException:
        raise HTTPNotFound(content={
            'reason': 'Task not found'
        })
