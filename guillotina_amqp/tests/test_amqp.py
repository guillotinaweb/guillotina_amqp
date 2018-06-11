from guillotina import app_settings
from guillotina_amqp.utils import add_task


async def _test_func(one, two, one_keyword=None):
    return one + two


async def test_add_task(dummy_guillotina):
    await add_task(_test_func, 1, 2)
    assert len(
        app_settings['amqp']['connections']['default']['channel'].queued) == 1
