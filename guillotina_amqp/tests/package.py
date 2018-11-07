from guillotina import configure
from guillotina_amqp.utils import add_object_task
from guillotina_amqp.utils import add_task


async def task_foobar_yo(one, two, three='blah'):
    return one + two


async def task_object_write(ob, value):
    ob.title = value
    ob._p_register()
    return 'done!'


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
