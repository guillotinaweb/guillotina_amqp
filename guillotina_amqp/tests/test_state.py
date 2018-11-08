from guillotina_amqp.state import get_state_manager
from guillotina_amqp.exceptions import TaskAlreadyAcquired

import pytest


async def test_update_and_get_should_match(configured_state_manager):
    state_manager = get_state_manager()
    await state_manager.update('foo', {'state': 'bar'})
    data = await state_manager.get('foo')
    assert data['state'] == 'bar'


async def test_acquire_should_block_further_acquires(configured_state_manager):
    state_manager = get_state_manager()
    await state_manager.acquire('foo')
    with pytest.raises(TaskAlreadyAcquired):
        await state_manager.acquire('foo')
    # Release it and acquire again
    await state_manager.release('foo')
    await state_manager.acquire('foo')


async def test_list_should_yield_all_items(configured_state_manager):
    state_manager = get_state_manager()
    tasks = set({'t1', 't2', 't3', 't4', 't5'})
    for task_id in tasks:
        await state_manager.update(task_id, {'state': f'{task_id} is OK!'})
    assert set([tid async for tid in state_manager.list()]) == tasks


async def test_cancel_should_put_tasks_in_cancelation_list(configured_state_manager):
    state_manager = get_state_manager()
    await state_manager.update('foo', {'status': 'bar'})
    canceled_list = [tid async for tid in state_manager.cancelation_list()]
    assert 'foo' not in canceled_list
    await state_manager.cancel('foo')
    canceled_list = [tid async for tid in state_manager.cancelation_list()]
    assert 'foo' in canceled_list


async def test_clean_cancel_should_clean_from_canceled_list(configured_state_manager):
    state_manager = get_state_manager()
    await state_manager.cancel('foo')
    canceled_list = [tid async for tid in state_manager.cancelation_list()]
    assert 'foo' in canceled_list
    await state_manager.clean_canceled('foo')
    canceled_list = [tid async for tid in state_manager.cancelation_list()]
    assert 'foo' not in canceled_list


#async def test_task_
