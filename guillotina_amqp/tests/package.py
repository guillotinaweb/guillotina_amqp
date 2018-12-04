from guillotina import configure
from guillotina_amqp.utils import add_object_task
from guillotina_amqp.utils import add_task
from guillotina_amqp.interfaces import MessageType


async def task_foobar_yo(one, two, three='blah'):
    return one + two


async def task_object_write(ob, value):
    ob.title = value
    ob._p_register()
    return 'done!'


async def task_object_write_generator(ob, values=None):
    for value in values or []:
        # This will be accumulated in the 'eventlog' key
        yield (MessageType.DEBUG, f'debug-{value}')
        # This will be accumulated in the 'result' key
        yield (MessageType.RESULT, value)
    if values:
        # Change the title of the object
        ob.title = 'CHANGED!'
        ob._p_register()


@configure.service(name='@foobar', method='GET')
async def foobar(context, request):
    # Endpoint to be used in tests to add a function task
    return {
        'task_id': (
            await add_task(task_foobar_yo, 1, 2, three='hello!')
        ).task_id
    }


@configure.service(name='@foobar-write', method='GET')
async def foobar_write(context, request):
    # Endpoint to be used in tests to add an object function task
    return {
        'task_id': (
            await add_object_task(task_object_write, context, 'Foobar written')
        ).task_id
    }


@configure.service(name='@foobar-write-async-gen', method='GET')
async def foobar_write_async_gen(context, request):
    # Endpoint to be used in tests to add an object function task
    values = ['one', 'two', 'final']
    return {
        'task_id': (
            await add_object_task(task_object_write_generator, context, values)
        ).task_id
    }
