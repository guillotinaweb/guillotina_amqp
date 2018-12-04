from guillotina_amqp.decorators import task
from guillotina_amqp.interfaces import MessageType
import asyncio


async def _test_func(one, two, one_keyword=None):
    return one + two


async def _test_asyncgen(one, two, one_keyword=None):
    yield (MessageType.DEBUG, 'Starting task')
    yield (MessageType.DEBUG, 'Yellow')
    yield (MessageType.RESULT, one + two)


async def _test_asyncgen_invalid():
    yield (MessageType.DEBUG, 1, 2, 3)
    yield (MessageType.RESULT)


async def _test_asyncgen_doubley(arg1, arg2, arg3):
    yield (MessageType.RESULT, arg1)
    yield (MessageType.RESULT, arg2)
    yield (MessageType.DEBUG, 'OK')
    yield (MessageType.RESULT, arg3)


@task
async def _test_long_func(duration):
    print('Started task')
    await asyncio.sleep(duration)
    print('Finished task')
    return 'done!'


@task
async def _test_failing_func():
    raise Exception('Foobar')


@task
async def _decorator_test_func(one, two):
    return one + two
