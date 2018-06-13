from zope.interface import Interface


class IStateManagerUtility(Interface):
    async def update(task_id, data):
        pass
