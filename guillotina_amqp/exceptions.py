class TaskNotFinishedException(Exception):
    pass


class TaskNotFoundException(Exception):
    pass


class TaskAlreadyAcquired(Exception):
    pass


class TaskAlreadyCancelled(Exception):
    pass
