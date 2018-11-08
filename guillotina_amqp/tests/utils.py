from guillotina_amqp.decorators import task
import asyncio

async def _test_func(one, two, one_keyword=None):
    return one + two


@task
async def _test_long_func(duration):
    print('Started task')
    await asyncio.sleep(duration)
    print('Finished task')
    return 'done!'


@task
async def _decorator_test_func(one, two):
    return one + two
