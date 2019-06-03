from zope.interface import Attribute
from zope.interface import implementer
from zope.interface import interfaces


class IJobEvent(interfaces.IObjectEvent):
    job = Attribute('Job object')


class IJobFinishedEvent(IJobEvent):
    result = Attribute('Job result')


class IJobProgressEvent(IJobEvent):
    result = Attribute('Intermediate job result')


@implementer(IJobFinishedEvent)
class JobFinishedEvent:
    def __init__(self, job, result):
        self.job = job
        self.result = result


@implementer(IJobProgressEvent)
class JobProgressEvent:
    def __init__(self, job, result):
        self.job = job
        self.result = result
