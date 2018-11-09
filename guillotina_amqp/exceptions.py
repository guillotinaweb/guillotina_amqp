class TaskNotFinishedException(Exception):
    pass


class TaskNotFoundException(Exception):
    pass


class TaskAlreadyAcquired(Exception):
    pass


class TaskAlreadyCanceled(Exception):
    pass


class TaskAccessUnauthorized(Exception):
    pass


class TaskMaxRetriesReached(Exception):
    pass
