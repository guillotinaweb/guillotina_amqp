from functools import partial
from guillotina.transactions import get_transaction
from guillotina.utils import get_current_request
from guillotina_amqp.interfaces import ITaskDefinition
from guillotina_amqp.utils import add_object_task
from guillotina_amqp.utils import add_task
from zope.interface import implementer

import uuid


@implementer(ITaskDefinition)
class TaskDefinition:

    def __init__(self, func, retries=3):
        self.func = func
        self.retries = retries

    async def __call__(self, *args, _request=None, **kwargs):
        return await add_task(
            self.func, _request=_request, _retries=self.retries, *args, **kwargs)

    schedule = __call__

    def _get_request(self, request, kwargs):
        if request is None:
            if 'request' in kwargs:
                request = kwargs['request']
            else:
                request = get_current_request()
        return request

    def after_request(self, *args, _request=None, _name=None, **kwargs):
        request = self._get_request(_request, kwargs)
        kwargs['_request'] = request
        if _name is None:
            _name = str(uuid.uuid4())
        request.add_future(_name, partial(self.schedule, *args, **kwargs))

    def after_commit(self, *args, _request=None, **kwargs):
        request = self._get_request(_request, kwargs)
        txn = get_transaction(request)
        kwargs['_request'] = request
        txn.add_after_commit_hook(self.schedule, args=args, kwargs=kwargs)


class ObjectTaskDefinition(TaskDefinition):
    async def __call__(self, *args, _request=None, **kwargs):
        return await add_object_task(
            self.func, _request=_request, _retries=self.retries, *args, **kwargs)

    schedule = __call__


def task(func, retries=3):
    return TaskDefinition(func, retries)


def object_task(func, retries=3):
    return ObjectTaskDefinition(func, retries)
