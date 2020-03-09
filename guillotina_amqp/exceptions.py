class TaskNotFinishedException(Exception):
    pass


class TaskNotFoundException(Exception):
    pass


class TaskAlreadyAcquired(Exception):
    pass


class TaskAccessUnauthorized(Exception):
    pass


class TaskMaxRetriesReached(Exception):
    pass


class ObjectNotFoundException(Exception):
    pass


class AMQPConfigurationNotFoundError(Exception):
    pass
