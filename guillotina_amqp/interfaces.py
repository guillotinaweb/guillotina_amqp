from zope.interface import Attribute
from zope.interface import Interface


class IStateManagerUtility(Interface):
    async def update(task_id, data):
        """Updates data related to task id into state manager
        """
        raise NotImplementedError()

    async def get(self, task_id):
        """Gets whatever was stored in state manager for task_id
        """
        raise NotImplementedError()

    async def list(self):
        """
        Yields items from the list of stored items
        """
        raise NotImplementedError()

    async def cancel(self, task_id):
        """
        Sets task_id to the canceled set of tasks
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
