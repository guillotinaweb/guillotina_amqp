from guillotina_amqp.state import get_state_manager
from guillotina_amqp.exceptions import TaskAlreadyAcquired
from guillotina_amqp.exceptions import TaskAccessUnauthorized

import pytest
import asyncio


async def clear_cache(sm):
    await sm._clean()


async def test_update_and_get_should_match(configured_state_manager, loop):
    state_manager = get_state_manager(loop)
    await state_manager.update('foo', {'state': 'bar'})
    data = await state_manager.get('foo')
    assert data['state'] == 'bar'
    await clear_cache(state_manager)


async def test_acquire_should_block_further_acquires(configured_state_manager, loop):
    state_manager = get_state_manager(loop)
    FOREVER = 300
    await state_manager.acquire('foo', ttl=FOREVER)
    with pytest.raises(TaskAlreadyAcquired):
        await state_manager.acquire('foo', ttl=FOREVER)
    # Release it and acquire again
    await state_manager.release('foo')
    await state_manager.acquire('foo', ttl=FOREVER)
    await clear_cache(state_manager)


async def test_acquire_should_unblock_after_ttl(configured_state_manager, loop):
    state_manager = get_state_manager(loop)
    await state_manager.acquire('foo', ttl=1)
    with pytest.raises(TaskAlreadyAcquired):
        await state_manager.acquire('foo', ttl=300)
    # Wait for the ttl and check
    await asyncio.sleep(1.1)
    await state_manager.acquire('foo', ttl=1)
    await clear_cache(state_manager)


async def test_list_should_yield_all_items(configured_state_manager, loop):
    state_manager = get_state_manager(loop)
    tasks = set({'t1', 't2', 't3', 't4', 't5'})
    for task_id in tasks:
        await state_manager.update(task_id, {'state': f'{task_id} is OK!'})
    assert set([tid async for tid in state_manager.list()]) == tasks
    await clear_cache(state_manager)


async def test_cancel_should_put_tasks_in_cancelation_list(configured_state_manager, loop):
    state_manager = get_state_manager(loop)
    await state_manager.update('foo', {'status': 'bar'})
    canceled_list = [tid async for tid in state_manager.cancelation_list()]
    assert 'foo' not in canceled_list
    await state_manager.cancel('foo')
    canceled_list = [tid async for tid in state_manager.cancelation_list()]
    assert 'foo' in canceled_list
    await clear_cache(state_manager)


async def test_clean_cancel_should_clean_from_canceled_list(configured_state_manager, loop):
    state_manager = get_state_manager(loop)
    await state_manager.cancel('foo')
    canceled_list = [tid async for tid in state_manager.cancelation_list()]
    assert 'foo' in canceled_list
    await state_manager.clean_canceled('foo')
    canceled_list = [tid async for tid in state_manager.cancelation_list()]
    assert 'foo' not in canceled_list
    await clear_cache(state_manager)


async def test_is_canceled_should_return_true_only_on_canceled_tasks(configured_state_manager, loop):
    state_manager = get_state_manager(loop)
    await state_manager.cancel('foo')
    assert await state_manager.is_canceled('foo')
    assert not await state_manager.is_canceled('bar')
    await clear_cache(state_manager)


async def test_refresh_should_raise_if_task_is_not_yours(configured_state_manager, loop):
    state_manager = get_state_manager(loop)
    state_manager.worker_id = 'me'
    await state_manager.acquire('footask', ttl=120)
    await state_manager.refresh_lock('footask', ttl=20)
    with pytest.raises(TaskAccessUnauthorized):
        state_manager.worker_id = 'another_person'
        await state_manager.refresh_lock('footask', 120)
    state_manager.worker_id = 'me'
    await state_manager.refresh_lock('footask', 20)
    await clear_cache(state_manager)


async def test_release_should_raise_if_task_is_not_yours(configured_state_manager, loop):
    state_manager = get_state_manager(loop)
    state_manager.worker_id = 'me'
    await state_manager.acquire('footask', ttl=120)
    with pytest.raises(TaskAccessUnauthorized):
        state_manager.worker_id = 'another_person'
        await state_manager.release('footask')
    state_manager.worker_id = 'me'
    await state_manager.release('footask')
    await clear_cache(state_manager)
