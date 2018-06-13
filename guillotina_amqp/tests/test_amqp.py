from guillotina import app_settings
from guillotina_amqp.utils import add_task
import aiotask_context

import asyncio


async def _test_func(one, two, one_keyword=None):
    return one + two


async def test_add_task(dummy_request, amqp_worker):
    aiotask_context.set('request', dummy_request)
    await add_task(_test_func, 1, 2)
    channel = app_settings['amqp']['connections']['default']['channel']
    assert len(channel.protocol.queues['guillotina']) == 1
    aiotask_context.set('request', None)


async def test_run_task(dummy_request, amqp_worker):
    aiotask_context.set('request', dummy_request)
    await add_task(_test_func, 1, 2)
    await asyncio.sleep(0.08)
    assert amqp_worker.total_run == 1
    aiotask_context.set('request', None)
