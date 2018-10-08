from guillotina import app_settings
from guillotina import configure
from guillotina.component import get_utility
from guillotina_amqp.exceptions import TaskNotFinishedException
from guillotina_amqp.exceptions import TaskNotFoundException
from guillotina_amqp.interfaces import IStateManagerUtility
from lru import LRU
from zope.interface.interfaces import ComponentLookupError

import asyncio
import json
import logging


try:
    import aioredis
    from guillotina_rediscache.cache import get_redis_pool
except ImportError:
    aioredis = None


logger = logging.getLogger('guillotina_amqp')


@configure.utility(provides=IStateManagerUtility, name='dummy')
class DummyStateManager:

    async def update(self, task_id, data):
        pass

    async def get(self, task_id):
        pass


@configure.utility(provides=IStateManagerUtility, name='memory')
class MemoryStateManager:
    '''
    Meaningless for anyting other than tests
    '''

    def __init__(self, size=5):
        self._data = LRU(size)

    async def update(self, task_id, data):
        if task_id not in self._data:
            self._data[task_id] = data
        else:
            self._data[task_id].update(data)

    async def get(self, task_id):
        if task_id in self._data:
            return self._data[task_id]


_EMPTY = object()


def get_state_manager():
    try:
        return get_utility(
            IStateManagerUtility,
            name=app_settings['amqp'].get('persistent_manager', 'dummy'))
    except ComponentLookupError:
        from guillotina_amqp.state import DummyStateManager
        return DummyStateManager()


@configure.utility(provides=IStateManagerUtility, name='redis')
class RedisStateManager:
    '''
    Meaningless for anyting other than tests
    '''
    _cache_prefix = 'amqpjobs-'

    def __init__(self):
        self._cache = None

    async def get_cache(self):
        if self._cache == _EMPTY:
            return None

        if aioredis is None:
            logger.warning('guillotina_rediscache not installed')
            self._cache = _EMPTY
            return None

        if 'redis' in app_settings:
            self._cache = aioredis.Redis(await get_redis_pool())
            return self._cache
        else:
            self._cache = _EMPTY
            return None

    async def update(self, task_id, data):
        cache = await self.get_cache()
        if cache:
            value = data
            existing = await cache.get(self._cache_prefix + task_id)
            if existing:
                value = json.loads(existing)
                value.update(data)
            await cache.set(
                self._cache_prefix + task_id, json.dumps(value), expire=60 * 60 * 1)

    async def get(self, task_id):
        cache = await self.get_cache()
        if cache:
            value = await cache.get(self._cache_prefix + task_id)
            if value:
                return json.loads(value)


class TaskState:

    def __init__(self, task_id):
        self.task_id = task_id

    async def join(self, wait=0.5):
        util = get_state_manager()
        while True:
            data = await util.get(self.task_id)
            if data is None:
                raise TaskNotFoundException(self.task_id)
            if data.get('status') in ('finished', 'errored'):
                return data
            await asyncio.sleep(wait)

    async def get_state(self):
        util = get_state_manager()
        data = await util.get(self.task_id)
        if data is None:
            raise TaskNotFoundException(self.task_id)
        return data

    async def get_status(self):
        '''
        possible statuses:
        - scheduled
        - consumed
        - running
        - finished
        - errored
        '''
        util = get_state_manager()
        data = await util.get(self.task_id)
        if data is None:
            raise TaskNotFoundException(self.task_id)
        return data.get('status')

    async def get_result(self):
        util = get_state_manager()
        data = await util.get(self.task_id)
        if data is None:
            raise TaskNotFoundException(self.task_id)
        if data.get('status') not in ('finished', 'errored'):
            raise TaskNotFinishedException(self.task_id)
        return data.get('result')
