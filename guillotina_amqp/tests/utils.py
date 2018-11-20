from guillotina_amqp.decorators import task
import asyncio


async def _test_func(one, two, one_keyword=None):
    return one + two


async def _test_asyncgen(one, two, one_keyword=None):
    yield (1, 'Starting task')
    yield (1, 'Yellow')
    yield (0, one + two)


async def _test_asyncgen_invalid():
    yield (1, 1, 2, 3)
    yield (0)


async def _test_asyncgen_doubley(arg1, arg2, arg3):
    yield (0, arg1)
    yield (0, arg2)
    yield (1, 'OK')
    yield (0, arg3)


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
