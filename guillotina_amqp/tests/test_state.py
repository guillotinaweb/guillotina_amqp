from guillotina_amqp.exceptions import TaskAccessUnauthorized
from guillotina_amqp.exceptions import TaskAlreadyAcquired
from guillotina_amqp.state import get_state_manager

import asyncio
import asynctest
import pytest


async def clear_cache(sm):
    await sm._clean()


async def test_update_and_get_should_match(configured_state_manager, loop):
    state_manager = get_state_manager(loop)
    await state_manager.update("foo", {"state": "bar"})
    data = await state_manager.get("foo")
    assert data["state"] == "bar"
    await clear_cache(state_manager)


async def test_acquire_should_block_further_acquires(configured_state_manager, loop):
    state_manager = get_state_manager(loop)
    FOREVER = 300
    await state_manager.acquire("foo", ttl=FOREVER)
    with pytest.raises(TaskAlreadyAcquired):
        await state_manager.acquire("foo", ttl=FOREVER)
    # Release it and acquire again
    await state_manager.release("foo")
    await state_manager.acquire("foo", ttl=FOREVER)
    await clear_cache(state_manager)


async def test_acquire_should_unblock_after_ttl(configured_state_manager, loop):
    state_manager = get_state_manager(loop)
    await state_manager.acquire("foo", ttl=1)
    with pytest.raises(TaskAlreadyAcquired):
        await state_manager.acquire("foo", ttl=300)
    # Wait for the ttl and check
    await asyncio.sleep(1.1)
    await state_manager.acquire("foo", ttl=1)
    await clear_cache(state_manager)


async def test_list_should_yield_all_items(configured_state_manager, loop):
    state_manager = get_state_manager(loop)
    tasks = set({"t1", "t2", "t3", "t4", "t5"})
    for task_id in tasks:
        await state_manager.update(task_id, {"state": f"{task_id} is OK!"})
    assert set([tid async for tid in state_manager.list()]) == tasks
    await clear_cache(state_manager)


async def test_is_canceled_should_return_true_only_on_canceled_tasks(
    configured_state_manager, loop
):
    state_manager = get_state_manager(loop)
    await state_manager.cancel("foo")
    assert await state_manager.is_canceled("foo")
    assert not await state_manager.is_canceled("bar")
    await clear_cache(state_manager)


async def test_refresh_should_raise_if_task_is_not_yours(
    configured_state_manager, loop
):
    state_manager = get_state_manager(loop)
    state_manager.worker_id = "me"
    await state_manager.acquire("footask", ttl=120)
    await state_manager.refresh_lock("footask", ttl=20)
    with pytest.raises(TaskAccessUnauthorized):
        state_manager.worker_id = "another_person"
        await state_manager.refresh_lock("footask", 120)
    state_manager.worker_id = "me"
    await state_manager.refresh_lock("footask", 20)
    await clear_cache(state_manager)


async def test_release_should_raise_if_task_is_not_yours(
    configured_state_manager, loop
):
    state_manager = get_state_manager(loop)
    state_manager.worker_id = "me"
    await state_manager.acquire("footask", ttl=120)
    with pytest.raises(TaskAccessUnauthorized):
        state_manager.worker_id = "another_person"
        await state_manager.release("footask")
    state_manager.worker_id = "me"
    await state_manager.release("footask")
    await clear_cache(state_manager)


async def test_task_state_disappears_after_ttl(redis_state_manager, loop):
    state_manager = get_state_manager(loop)
    await state_manager.update("foo", {"state": "bar"}, ttl=2)
    data = await state_manager.get("foo")
    assert data["state"] == "bar"
    await asyncio.sleep(2)
    data = await state_manager.get("foo")
    assert not data
    await clear_cache(state_manager)


class MockedRedisGET:
    def __init__(self):
        self.called = 0

    async def __call__(self, *args, **kw):
        self.called += 1
        raise ConnectionResetError


async def test_connection_reset_errors_are_retried(redis_state_manager, loop):
    state_manager = get_state_manager(loop)
    mocked = MockedRedisGET()
    with asynctest.mock.patch("guillotina_amqp.state.aioredis.Redis.get", new=mocked):

        with pytest.raises(ConnectionResetError):
            await state_manager.get("foo")

        assert mocked.called == 4
