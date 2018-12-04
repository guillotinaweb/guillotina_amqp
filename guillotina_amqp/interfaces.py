from zope.interface import Attribute
from zope.interface import Interface


class MessageType(object):
    """Encapsulates constants message types"""
    RESULT = 'result'
    DEBUG = 'debug'


class IStateManagerUtility(Interface):
    async def update(task_id, data):
        """Updates data related to task id into state manager
        """
        raise NotImplementedError()

    async def get(self, task_id):
        """Gets whatever was stored in state manager for task_id
        """
        raise NotImplementedError()

    async def exists(self, task_id):
        """Returns whether a task id exists in the state manager
        """
        raise NotImplementedError()

    async def list(self):
        """
        Yields items from the list of stored items
        """
        raise NotImplementedError()

    async def is_mine(self, task_id):
        """Returns whether the worker has a lock on a given task_idq
        """
        raise NotImplementedError()

    async def cancel(self, task_id):
        """
        Sets task_id to the canceled set of tasks
        """
        raise NotImplementedError()

    async def acquire(self, task_id, ttl):
        """
        Get a lock on a certain task, by id.
        """
        raise NotImplementedError()

    async def release(self, task_id):
        """
        Release the lock for the specified task
        """
        raise NotImplementedError()

    async def refresh_lock(self, task_id, ttl):
        """
        Update lock TTL
        """
        raise NotImplementedError()

    async def cancelation_list(self):
        """
        Yields items from the canceled set
        """
        raise NotImplementedError()

    async def clean_canceled(self, task_id):
        """
        Removes a task from the canceled set
        """
        raise NotImplementedError()

    async def is_canceled(self, task_id):
        """
        Whether a task id has been cancelled
        """
        raise NotImplementedError()


class ITaskDefinition(Interface):
    func = Attribute('actual function to run')

    async def __call__(*args, _request=None, **kwargs):
        '''
        schedule it
        '''

    def after_request(*args, _request=None, _name=None, **kwargs):
        '''
        schedule after request
        '''

    def after_commit(*args, _request=None, **kwargs):
        '''
        schedule after commit
        '''
