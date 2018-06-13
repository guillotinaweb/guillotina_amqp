from guillotina import app_settings
from guillotina_amqp.decorators import task
from guillotina_amqp.state import TaskState
from guillotina_amqp.utils import add_task

import aiotask_context
import asyncio
import json


async def _test_func(one, two, one_keyword=None):
    return one + two


@task
async def _decorator_test_func(one, two):
    return one + two


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
