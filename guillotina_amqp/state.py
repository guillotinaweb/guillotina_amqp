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
import copy

try:
    import aioredis
    from guillotina_rediscache.cache import get_redis_pool
except ImportError:
    aioredis = None


logger = logging.getLogger('guillotina_amqp')

DEFAULT_LOCK_TTL_S = 120


@configure.utility(provides=IStateManagerUtility, name='memory')
class MemoryStateManager:
    '''
    Meaningless for anyting other than tests
    '''
    def __init__(self, size=10):
        self._data = LRU(size)
        self._locks = {}
        self._canceled = set()
        self.worker_id = uuid.uuid4().hex

    async def update(self, task_id, data):
        # Updates existing data with new data
        existing = await self.get(task_id)
        existing.update(data)
        self._data[task_id] = existing

    async def get(self, task_id):
        return self._data.get(task_id, {})

    async def exists(self, task_id):
        return task_id in self._data

    async def list(self):
        for task_id in self._data.keys():
            yield task_id

    async def acquire(self, task_id, ttl):
        already_locked = await self.is_locked(task_id)
        if already_locked:
            raise TaskAlreadyAcquired(task_id)

        # Set new lock
        from guillotina_amqp.utils import TimeoutLock
        lock = TimeoutLock(self.worker_id)
        lock.acquire(ttl=ttl)
        self._locks[task_id] = lock

    async def is_locked(self, task_id):
        if task_id not in self._locks:
            return False
        return self._locks[task_id].locked()

    async def release(self, task_id):
        if await self.is_locked(task_id):
            # Release lock
            self._locks[task_id].release()
        # Remove it from data structure
        self._locks.pop(task_id, None)

    async def refresh_lock(self, task_id, ttl):
        if task_id not in self._locks:
            raise Exception(f'{task_id} not found')
        lock = self._locks[task_id]
        if lock.worker_id != self.worker_id:
            raise Exception(f"You can't refresh a lock that's not yours")
        return self._locks[task_id].refresh_lock(ttl)

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

    async def is_canceled(self, task_id):
        return task_id in self._canceled


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
                # Update existing with new data
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
        return {}

    async def exists(self, task_id):
        data = await self.get(task_id)
        return data is not None

    async def list(self):
        cache = await self.get_cache()
        async for key in cache.iscan(match=f'{self._cache_prefix}*'):
            yield key.decode().replace(self._cache_prefix, '')

    async def acquire(self, task_id, ttl):
        cache = await self.get_cache()
        resp = await cache.setnx(f'lock:{task_id}', self.worker_id)
        if not resp:
            raise TaskAlreadyAcquired(task_id)

        # Need to set an expiration for the lock in redis at
        # creation time
        refreshed = await self.refresh_lock(task_id, ttl)
        return refreshed

    async def release(self, task_id):
        cache = await self.get_cache()
        resp = await cache.delete(f'lock:{task_id}')
        return resp > 0

    async def refresh_lock(self, task_id, ttl):
        cache = await self.get_cache()
        resp = await cache.get(f'lock:{task_id}')
        if not resp:
            raise Exception(f"Lock for {task_id} doesn't exist")
        if resp != self.worker_id:
            raise Exception(f"Can't refresh a task lock that's not yours")
        resp = await cache.expire(f'lock:{task_id}', ttl)
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

    async def is_canceled(self, task_id):
        cache = await self.get_cache()
        value = await cache.get(f'{self._cache_prefix}cancel' + task_id)
        return value is not None


class TaskState:
    """Wrapper around state_manager implementation so we can use it by
    just having a task_id
    """

    def __init__(self, task_id):
        self.task_id = task_id

    async def join(self, wait=0.5):
        util = get_state_manager()
        while True:
            data = await util.get(self.task_id)
            if not data:
                raise TaskNotFoundException(self.task_id)
            if data.get('status') in ('finished', 'errored'):
                return data
            await asyncio.sleep(wait)

    async def get_state(self):
        util = get_state_manager()
        data = await util.get(self.task_id)
        if not data:
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
        if not data:
            raise TaskNotFoundException(self.task_id)
        return data.get('status')

    async def get_result(self):
        util = get_state_manager()
        data = await util.get(self.task_id)
        if not data:
            raise TaskNotFoundException(self.task_id)
        if data.get('status') not in ('finished', 'errored'):
            raise TaskNotFinishedException(self.task_id)
        return data.get('result')

    async def cancel(self):
        util = get_state_manager()
        return await util.cancel(self.task_id)

    async def acquire(self, ttl=DEFAULT_LOCK_TTL_S):
        util = get_state_manager()
        try:
            await util.acquire(self.task_id, ttl)
        except TaskAlreadyAcquired:
            logger.warning(f'Task {self.task_id} is already taken')
            return False
        else:
            return True

    async def refresh_lock(self, ttl=DEFAULT_LOCK_TTL_S):
        util = get_state_manager()
        await util.refresh_lock(self.task_id, ttl)

    async def release(self):
        util = get_state_manager()
        await util.release(self.task_id)

    async def is_canceled(self):
        util = get_state_manager()
        return await util.is_canceled(self.task_id)
