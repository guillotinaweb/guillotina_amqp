from zope.interface import Attribute
from zope.interface import Interface


class IStateManagerUtility(Interface):
    async def update(task_id, data):
        pass

    async def get(self, task_id):
        pass

    async def list(self):
        pass

    # Communication client -> worker needs to be do throught a shared
    # resource. i.e. distributed lock or a pub/sub channel on redis.
    async def cancel_task(self, task_id):
        pass

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
