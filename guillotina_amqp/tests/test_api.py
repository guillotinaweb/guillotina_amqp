from guillotina import task_vars
from guillotina.tests.utils import get_container
from guillotina_amqp.exceptions import TaskNotFoundException
from guillotina_amqp.state import get_state_manager
from guillotina_amqp.state import TaskState
from guillotina_amqp.tests.utils import _test_func
from guillotina_amqp.utils import add_task

import asynctest
import json
import pytest


pytestmark = pytest.mark.asyncio


async def test_list_tasks_returns_all_tasks(container_requester, dummy_request):
    async with container_requester as requester:
        task_vars.request.set(dummy_request)
        task_vars.db.set(requester.db)
        await get_container(requester=requester)

        # Add some tasks first
        t1 = await add_task(_test_func, 1, 2)
        t2 = await add_task(_test_func, 3, 4)

        resp, status = await requester("GET", "/db/guillotina/@amqp-tasks")
        assert status == 200
        assert len(resp) == 2
        assert t1.task_id in resp
        assert t2.task_id in resp


async def test_info_task(container_requester, dummy_request):
    async with container_requester as requester:
        task_vars.request.set(dummy_request)
        task_vars.db.set(requester.db)
        await get_container(requester=requester)

        # Add some tasks first
        t1 = await add_task(_test_func, 1, 2)

        # Check returns correctly if existing task_id
        resp, status = await requester(
            "GET", f"/db/guillotina/@amqp-tasks/{t1.task_id}"
        )
        assert status == 200
        assert resp["status"] == "scheduled"

        # Check returns 404 if task_id is not known
        resp, status = await requester("GET", "/db/guillotina/@amqp-tasks/foo")
        assert status == 404
        assert resp["reason"] == "Task not found"

        resp, status = await requester(
            "GET", "/db/guillotina/@amqp-tasks/task:db-guillotina-foobar"
        )
        assert status == 404
        assert resp["reason"] == "Task not found"


async def test_cancel_task(container_requester, dummy_request):

    async with container_requester as requester:
        task_vars.request.set(dummy_request)
        task_vars.db.set(requester.db)
        await get_container(requester=requester)

        # Add some tasks first
        t1 = await add_task(_test_func, 1, 2)

        # Check returns correctly if existing task_id
        resp, status = await requester(
            "DELETE", f"/db/guillotina/@amqp-tasks/{t1.task_id}"
        )
        assert status == 200
        assert resp is True

        # Check returns correctly if already canceled task_id
        resp, status = await requester(
            "DELETE", f"/db/guillotina/@amqp-tasks/{t1.task_id}"
        )
        assert status == 200
        assert resp is True

        # Check returns 404 is unknown task
        resp, status = await requester("DELETE", "/db/guillotina/@amqp-tasks/foo")
        assert status == 404

    task_vars.request.set(None)


@asynctest.patch("guillotina_amqp.api.can_debug_amqp")
async def test_info_task_filtered_response(
    can_debug_amqp, container_requester, dummy_request
):
    async with container_requester as requester:
        task_vars.request.set(dummy_request)
        task_vars.db.set(requester.db)
        await get_container(requester=requester)

        # Add some tasks first
        t1 = await add_task(_test_func, 1, 2)

        # Update task with job data
        state_manager = get_state_manager()
        await state_manager.update(t1.task_id, {"job_data": {"foo": "bar"}})

        # When the user has debug permission, the response contains job_data
        can_debug_amqp.return_value = True
        resp, status = await requester(
            "GET", f"/db/guillotina/@amqp-tasks/{t1.task_id}"
        )
        assert status == 200
        assert "job_data" in resp

        # When the user has not debug permission, the response does not contain job_data
        can_debug_amqp.return_value = False
        resp, status = await requester(
            "GET", f"/db/guillotina/@amqp-tasks/{t1.task_id}"
        )
        assert status == 200
        assert "job_data" not in resp


async def join_amqp_worker(task_id=None):
    """Wait until amqp worker has finished executing a specific
    task_id. If no task_id is specified, will wait for all scheduled
    tasks.

    """
    sm = get_state_manager()
    if task_id is not None:
        # Wait for specific task
        await wait_for_task(task_id)
        return
    # Wait for all scheduled tasks
    async for task_id in sm.list():
        await wait_for_task(task_id)


async def wait_for_task(task_id: str) -> bool:
    task = TaskState(task_id)
    try:
        await task.join()
        return True
    except TaskNotFoundException:
        return False


async def test_example_of_service_spawning_a_task(container_requester, amqp_worker):
    async with container_requester as requester:
        # Create item in container with a title
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Item", "title": "Foo"}),
        )
        assert status == 201
        item_url = "/db/guillotina/" + resp["@name"]

        # Schedule task to clear title of the it
        resp, status = await requester("POST", item_url + "/@clearTitle")
        assert status == 200
        task_id = resp["task_id"]

        # Wait until task has finished
        assert await wait_for_task(task_id) is True
        resp, status = await requester("GET", f"/db/guillotina/@amqp-tasks/{task_id}")
        assert status == 200
        assert resp["status"] == "finished"

        #  Check that title was overwritten
        resp, status = await requester("GET", item_url)
        assert status == 200
        assert resp["title"] is None
