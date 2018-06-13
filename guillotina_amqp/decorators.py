from guillotina_amqp.utils import add_object_task
from guillotina_amqp.utils import add_task


def task(original_func, retries=3):
    async def new_func(*args, **kwargs):
        return await add_task(original_func, _retries=retries, *args, **kwargs)
    new_func.__real_func__ = original_func
    return new_func


def object_task(original_func, retries=3):
    async def new_func(*args, **kwargs):
        return await add_object_task(original_func, _retries=retries, *args, **kwargs)
    new_func.__real_func__ = original_func
    return new_func
