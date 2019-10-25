from guillotina import task_vars
from guillotina.tests.utils import get_container
from guillotina_amqp.tests.utils import _test_func
from guillotina_amqp.utils import add_task


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
