from zope.interface import Attribute
from zope.interface import Interface


class IStateManagerUtility(Interface):
    async def update(task_id, data):
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
