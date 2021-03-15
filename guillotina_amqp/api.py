from .exceptions import TaskNotFoundException
from .state import get_state_manager
from .state import TaskState
from .utils import get_task_id_prefix
from guillotina import configure
from guillotina.interfaces import IContainer
from guillotina.response import HTTPNotFound
from guillotina.utils import get_security_policy


def can_debug_amqp(context: IContainer) -> bool:
    security = get_security_policy()
    return security.check_permission("guillotina.DebugAMQP", context)


@configure.service(
    method="GET",
    name="@amqp-tasks",
    context=IContainer,
    permission="guillotina.ManageAMQP",
    summary="Returns the list of running tasks",
)
@configure.service(
    method="GET",
    name="@amqp-info",
    context=IContainer,
    permission="guillotina.ManageAMQP",
    summary="Deprecated: Returns the list of running tasks",
)
async def list_tasks(context, request):
    mngr = get_state_manager()
    ret = []
    task_prefix = get_task_id_prefix()
    async for el in mngr.list():
        if el.startswith(task_prefix):
            ret.append(el)
    return ret


@configure.service(
    method="GET",
    name="@amqp-tasks/{task_id}",
    context=IContainer,
    permission="guillotina.AccessContent",
    summary="Shows the info of a given task id",
)
@configure.service(
    method="GET",
    name="@amqp-info/{task_id}",
    context=IContainer,
    permission="guillotina.AccessContent",
    summary="Deprecated: Shows the info of a given task id",
)
async def info_task(context, request):
    task_prefix = get_task_id_prefix()
    if not request.matchdict["task_id"].startswith(task_prefix):
        return HTTPNotFound(content={"reason": "Task not found"})
    try:
        task = TaskState(request.matchdict["task_id"])
        state = await task.get_state()
        if not can_debug_amqp(context):
            state.pop("job_data", None)
        return state
    except TaskNotFoundException:
        return HTTPNotFound(content={"reason": "Task not found"})


@configure.service(
    method="DELETE",
    name="@amqp-tasks/{task_id}",
    context=IContainer,
    permission="guillotina.ManageAMQP",
    summary="Cancel a specific task by id",
)
@configure.service(
    method="DELETE",
    name="@amqp-cancel/{task_id}",
    context=IContainer,
    permission="guillotina.ManageAMQP",
    summary="Deprecated: Cancel a specific task by id",
)
async def cancel_task(context, request):
    task_prefix = get_task_id_prefix()
    if not request.matchdict["task_id"].startswith(task_prefix):
        return HTTPNotFound(content={"reason": "Task not found"})
    task = TaskState(request.matchdict["task_id"])
    try:
        return await task.cancel()
    except TaskNotFoundException:
        return HTTPNotFound(content={"reason": "Task not found"})
