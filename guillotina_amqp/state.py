from guillotina import app_settings
from guillotina import configure
from guillotina.component import get_utility
from guillotina_amqp.exceptions import TaskNotFinishedException
from guillotina_amqp.exceptions import TaskNotFoundException
from guillotina_amqp.exceptions import TaskAlreadyAcquired

from guillotina_amqp.interfaces import IStateManagerUtility
from lru import LRU

import asyncio
import json
import logging
import time
import uuid
from threading import Lock
import copy

try:
    import aioredis
    from guillotina_rediscache.cache import get_redis_pool
except ImportError:
    aioredis = None


logger = logging.getLogger('guillotina_amqp')



@configure.utility(provides=IStateManagerUtility, name='memory')
class MemoryStateManager:
    '''
    Meaningless for anyting other than tests
    '''
    def __init__(self, size=5):
        self._data = LRU(size)
        self._locks = {}
        self._canceled = set()

    async def update(self, task_id, data):
        if task_id not in self._data:
            self._data[task_id] = data
        else:
            self._data[task_id].update(data)

    async def get(self, task_id):
        if task_id in self._data:
            return self._data[task_id]

    async def list(self):
        for task_id in self._data.keys():
            yield task_id

    async def acquire(self, task_id, ttl=None):
        already_locked = await self._locked(task_id)
        if already_locked:
            raise TaskAlreadyAcquired(task_id)
        # Set new lock
        if ttl:
            from guillotina_amqp.utils import TimeoutLock
            lock = TimeoutLock()
            lock.acquire(timeout=ttl)
        else:
            lock = Lock()
            lock.acquire()
        self._locks[task_id] = lock

    async def _locked(self, task_id):
        if task_id not in self._locks:
            return False

        return self._locks[task_id].locked()

    async def release(self, task_id):
        if task_id not in self._locks:
            # No need to do here
            return

        if not self._locks[task_id].locked():
            self._locks.pop(task_id)
            return

        # Release lock
        self._locks[task_id].release()
        self._locks.pop(task_id)

    async def cancel(self, task_id):
        self._canceled.update({task_id})
        return True

    async def cancelation_list(self):
        canceled = copy.deepcopy(self._canceled)
        for task_id in canceled:
            yield task_id

    async def clean_canceled(self, task_id):
        try:
            self._canceled.remove(task_id)
            return True
        except KeyError:
            # Task id wasn't canceled
            return False



_EMPTY = object()


def get_state_manager():
    """Factory that gets the configured state manager.

    Currently we have two implementations: memory | redis
    """
    return get_utility(
        IStateManagerUtility,
        name=app_settings['amqp'].get('persistent_manager', 'memory')
    )


@configure.utility(provides=IStateManagerUtility, name='redis')
class RedisStateManager:
    '''Implementation of the IStateManagerUtility with Redis
    '''
    _cache_prefix = 'amqpjobs-'

    def __init__(self):
        self._cache = None
        self.worker_id = uuid.uuid4().hex

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
                self._cache_prefix + task_id, json.dumps(value))

    async def get(self, task_id):
        cache = await self.get_cache()
        if cache:
            value = await cache.get(self._cache_prefix + task_id)
            if value:
                return json.loads(value)

    async def list(self):
        cache = await self.get_cache()
        async for key in cache.iscan(match=f'{self._cache_prefix}*'):
            yield key.decode().replace(self._cache_prefix, '')

    async def acquire(self, task_id, ttl=None):
        cache = await self.get_cache()
        resp = await cache.setnx(f'lock:{task_id}', self.worker_id)
        if not resp:
            raise TaskAlreadyAcquired(task_id)
        elif ttl:
            await cache.expire(f'lock:{task_id}', ttl)

    async def release(self, task_id):
        cache = await self.get_cache()
        resp = await cache.delete(f'lock:{task_id}')
        return resp > 0

    async def cancel(self, task_id):
        cache = await self.get_cache()
        current_time = time.time()
        resp = await cache.zadd(f'{self._cache_prefix}cancel',
                                current_time, task_id)
        return resp > 0

    async def cancelation_list(self):
        cache = await self.get_cache()
        async for val, score in cache.izscan(f'{self._cache_prefix}cancel'):
            yield val.decode()

    async def clean_canceled(self, task_id):
        cache = await self.get_cache()
        resp = await cache.zrem(f'{self._cache_prefix}cancel', task_id)
        return resp > 0


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

    async def cancel(self):
        util = get_state_manager()
        return await util.cancel(self.task_id)

    async def acquire(self, timeout=120):
        util = get_state_manager()
        try:
            await util.acquire(self.task_id, timeout)
        except TaskAlreadyAcquired:
            logger.warning(f'Task {self.task_id} is already taken')
            return False
        else:
            return True

    async def release(self):
        util = get_state_manager()
        await util.release(self.task_id)
