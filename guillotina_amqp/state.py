from guillotina import app_settings
from guillotina import configure
from guillotina.component import get_utility
from guillotina_amqp.exceptions import TaskNotFinishedException
from guillotina_amqp.exceptions import TaskNotFoundException
from guillotina_amqp.exceptions import TaskAlreadyAcquired
from guillotina_amqp.exceptions import TaskAccessUnauthorized

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


logger = logging.getLogger('guillotina_amqp.state')

DEFAULT_LOCK_TTL_S = 60 * 1  # 1 minute


@configure.utility(provides=IStateManagerUtility, name='memory')
class MemoryStateManager:
    '''
    Meaningless for anyting other than tests
    '''
    def __init__(self, size=10):
        self.size = size
        self._data = LRU(self.size)
        self._locks = {}
        self._canceled = set()
        self.worker_id = uuid.uuid4().hex

    def set_loop(self, loop=None):
        pass

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
        await lock.acquire(ttl=ttl)
        self._locks[task_id] = lock

    async def is_mine(self, task_id):
        if task_id not in self._locks:
            raise TaskNotFoundException(task_id)
        lock = self._locks[task_id]
        return lock.locked() and lock.worker_id == self.worker_id

    async def is_locked(self, task_id):
        if task_id not in self._locks:
            return False
        return self._locks[task_id].locked()

    async def release(self, task_id):
        if not await self.is_mine(task_id):
            # You can't refresh a lock that's not yours
            raise TaskAccessUnauthorized(task_id)
        # Release lock and pop it from data structure
        self._locks[task_id].release()
        self._locks.pop(task_id, None)

    async def refresh_lock(self, task_id, ttl):
        if task_id not in self._locks:
            raise TaskNotFoundException(task_id)

        if not await self.is_locked(task_id):
            raise Exception(f'Task {task_id} is not locked')

        if not await self.is_mine(task_id):
            # You can't refresh a lock that's not yours
            raise TaskAccessUnauthorized(task_id)

        # Refresh
        return await self._locks[task_id].refresh_lock(ttl)

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

    async def _clean(self):
        self._data = LRU(self.size)
        self._locks = {}
        self._canceled = set()


_EMPTY = object()


def get_state_manager(loop=None):
    """Factory that gets the configured state manager.

    Currently we have two implementations: memory | redis
    """
    utility = get_utility(
        IStateManagerUtility,
        name=app_settings['amqp']['persistent_manager'],
    )
    if loop:
        utility.set_loop(loop)
    return utility


@configure.utility(provides=IStateManagerUtility, name='redis')
class RedisStateManager:
    '''Implementation of the IStateManagerUtility with Redis
    '''

    def __init__(self, loop=None):
        self._cache_prefix = app_settings.get('redis_prefix_key', 'amqpjobs-')
        self.loop = loop
        self._cache = None
        self.worker_id = uuid.uuid4().hex

    def lock_prefix(self, task_id):
        return f'{self._cache_prefix}lock:{task_id}'

    @property
    def cancel_prefix(self):
        return f'{self._cache_prefix}cancel'

    def set_loop(self, loop=None):
        if loop:
            self.loop = loop

    async def get_cache(self):
        if self._cache == _EMPTY:
            return None

        if aioredis is None:
            logger.warning('guillotina_rediscache not installed')
            self._cache = _EMPTY
            return None

        if 'redis' in app_settings:
            self._cache = aioredis.Redis(await get_redis_pool(loop=self.loop))
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
        if await self.is_locked(task_id):
            raise TaskAlreadyAcquired(task_id)

        # Set the lock
        cache = await self.get_cache()
        resp = await cache.setnx(self.lock_prefix(task_id), self.worker_id)
        if not resp:
            raise Exception(f'Error acquiring {task_id}')

        # Need to set an expiration for the lock in redis at creation
        # time
        refreshed = await self.refresh_lock(task_id, ttl)
        return refreshed

    async def is_locked(self, task_id):
        cache = await self.get_cache()
        resp = await cache.get(self.lock_prefix(task_id))
        return resp is not None

    async def is_mine(self, task_id):
        cache = await self.get_cache()
        task_owner_id = await cache.get(self.lock_prefix(task_id))
        if not task_owner_id:
            return False
        return task_owner_id.decode() == self.worker_id

    async def release(self, task_id):
        if not await self.is_locked(task_id):
            # There is no lock, nothing to do
            return False
        if not await self.is_mine(task_id):
            # You can't release a task for which you don't own a lock
            raise TaskAccessUnauthorized
        cache = await self.get_cache()
        resp = await cache.delete(self.lock_prefix(task_id))
        return resp > 0

    async def refresh_lock(self, task_id, ttl):
        if not await self.is_locked(task_id):
            # There is no lock, nothing to do
            return False

        if not await self.is_mine(task_id):
            # You can't release a task for which you don't own a lock
            raise TaskAccessUnauthorized(task_id)

        cache = await self.get_cache()
        resp = await cache.expire(self.lock_prefix(task_id), ttl)
        return resp > 0

    async def cancel(self, task_id):
        cache = await self.get_cache()
        current_time = time.time()
        resp = await cache.zadd(self.cancel_prefix,
                                current_time, task_id)
        return resp > 0

    async def cancelation_list(self):
        cache = await self.get_cache()
        async for val, score in cache.izscan(self.cancel_prefix):
            yield val.decode()

    async def clean_canceled(self, task_id):
        cache = await self.get_cache()
        resp = await cache.zrem(self.cancel_prefix, task_id)
        return resp > 0

    async def is_canceled(self, task_id):
        async for tid in self.cancelation_list():
            if tid == task_id:
                return True
        return False

    async def _clean(self):
        await self._cache.flushall()


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
            if data.get('status') in ('finished', 'errored', 'canceled'):
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
        - canceled
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
        if await util.is_canceled(self.task_id):
            # Already canceled
            return True
        if not await util.exists(self.task_id):
            raise TaskNotFoundException
        # Cancel it
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
