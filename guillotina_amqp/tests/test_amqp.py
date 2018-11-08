from guillotina import app_settings
from guillotina_amqp.state import TaskState
from guillotina_amqp.utils import add_task
from guillotina_amqp.utils import cancel_task
from guillotina_amqp.tests.utils import _test_func
from guillotina_amqp.tests.utils import _test_long_func
from guillotina_amqp.tests.utils import _decorator_test_func

import aiotask_context
import asyncio
import json


async def test_add_task(dummy_request, amqp_worker):
    aiotask_context.set('request', dummy_request)
    await add_task(_test_func, 1, 2)
    channel = app_settings['amqp']['connections']['default']['channel']
    assert len(channel.protocol.queues['guillotina']) == 1
    aiotask_context.set('request', None)


async def test_run_task(dummy_request, amqp_worker):
    aiotask_context.set('request', dummy_request)
    state = await add_task(_test_func, 1, 2)
    await state.join(0.01)
    await asyncio.sleep(0.1)  # prevent possible race condition here
    assert amqp_worker.total_run == 1
    assert await state.get_result() == 3
    aiotask_context.set('request', None)


async def test_task_from_service(amqp_worker, container_requester):
    async with container_requester as requester:
        resp, _ = await requester('GET', '/db/guillotina/@foobar')
        state = TaskState(resp['task_id'])
        await state.join(0.01)
        assert await state.get_result() == 3
        await asyncio.sleep(0.1)  # prevent possible race condition here
        assert amqp_worker.total_run == 1


async def test_task_commits_data_from_service(amqp_worker, container_requester):
    async with container_requester as requester:
        await requester('POST', '/db/guillotina', data=json.dumps({
            '@type': 'Item',
            'id': 'foobar',
            'title': 'blah'
        }))
        resp, _ = await requester('GET', '/db/guillotina/foobar/@foobar-write')
        state = TaskState(resp['task_id'])
        await state.join(0.01)
        assert await state.get_result() == 'done!'
        await asyncio.sleep(0.1)  # prevent possible race condition here
        assert amqp_worker.total_run == 1
        resp, status = await requester('GET', '/db/guillotina/foobar')
        assert resp['title'] == 'Foobar written'


async def test_cancels_long_running_task(dummy_request, amqp_worker, configured_state_manager):
    aiotask_context.set('request', dummy_request)
    # Add long running task
    ts = await _test_long_func(120)
    # Wait a bit and cancel
    await asyncio.sleep(1)
    success = await cancel_task(ts.task_id)
    assert success
    # Wait until worker sees task is cancelled and cancels the
    # asycio task
    await ts.join(0.2)

    # Check that the it was indeed cancelled
    state = await ts.get_state()
    assert state['status'] == 'errored'
    assert 'CancelledError' in state['error']
    await asyncio.sleep(0.1)  # prevent possible race condition here
    assert amqp_worker.total_run == 1
    aiotask_context.set('request', None)


async def test_decorator_task(dummy_request, amqp_worker):
    aiotask_context.set('request', dummy_request)
    state = await _decorator_test_func(1, 2)
    data = await state.join(0.01)
    assert data['result'] == 3
    await asyncio.sleep(0.1)  # prevent possible race condition here
    assert amqp_worker.total_run == 1
    assert await state.get_status() == 'finished'
    assert await state.get_result() == 3
    aiotask_context.set('request', None)
