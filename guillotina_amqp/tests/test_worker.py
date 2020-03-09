from guillotina_amqp.state import get_state_manager
from guillotina_amqp.tests.mocks import MockChannel
from guillotina_amqp.tests.mocks import MockEnvelope
from guillotina_amqp.worker import Worker

import json


async def test_instance_attributes_defaults(dummy_request):
    worker = Worker()
    assert worker.num_running == 0
    assert worker.total_run == 0
    assert worker.total_errored == 0
    assert worker.sleep_interval == 0.1


async def test_worker_acks_canceled_tasks(dummy_request):
    # Fake some task data
    task_id = "foo"
    task_data = json.dumps({"task_id": task_id, "func": "foo.bar"})

    # Set task as canceled in state
    state_manager = get_state_manager()
    assert await state_manager.cancel(task_id)

    channel = MockChannel()
    assert len(channel.acked) == 0
    envelope = MockEnvelope("footag")

    # Pretend worker picks up the task
    worker = Worker()
    await worker.handle_queued_job(channel, task_data, envelope, None)

    # Check that worker sent ack to amqp channel
    assert len(channel.acked) == 1
    assert channel.acked[0]["kwargs"]["delivery_tag"] == envelope.delivery_tag


async def test_worker_acks_already_acquired_tasks(dummy_request):
    # Fake some task data
    task_id = "foo"
    task_data = json.dumps({"task_id": task_id, "func": "foo.bar"})

    # Mock as if the task would be acquired
    state_manager = get_state_manager()
    await state_manager.acquire(task_id, 900)

    channel = MockChannel()
    assert len(channel.acked) == 0
    envelope = MockEnvelope("footag")

    # Pretend worker picks up the task
    worker = Worker()
    await worker.handle_queued_job(channel, task_data, envelope, None)

    # Check that worker sent ack to amqp channel
    assert len(channel.acked) == 1
    assert channel.acked[0]["kwargs"]["delivery_tag"] == envelope.delivery_tag
