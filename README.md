![](https://github.com/guillotinaweb/guillotina_amqp/.github/workflows/continuous-integration.yml/badge.svg)


# Plugin documentation

This plugin integrates aioamqp into guillotina, providing an execution framework for asyncio tasks:

- Guillotina command to start a worker: `amqp-worker`
- Workers consume tasks from rabbit-mq through the aioamqp integration
- Redis state manager implementation to keep a global view of running tasks
- Utilities and endpoints for adding new tasks and for task cancellation

Its distributed design - the absence of a central worker manager - makes it more robust. Task cancelation is signaled over the state manager, and workers will be responsible for stopping canceled tasks.

A watchdog on the asyncio loop can be launched with the `auto-kill-timeout` command argument, which will kill the worker if one of its tasks has captured the loop for too long.

When a task fails, the worker will send it to the delay queue, which has been configured to re-queue tasks to the main queue after a certain TTL. Failed tasks are retried a limited amount of times.

## Configuration

Example docs
```json
    {
        "amqp": {
            "host": "localhost",
            "port": 5673,
            "login": "guest",
            "password": "guest",
            "vhost": "/",
            "heartbeat": 800,
            "queue": "guillotina",
            "persistent_manager": "redis",
            "delayed_ttl_ms": 60000,
            "errored_ttl_ms": 604800000,
        }
    }
```

- `host` and `port`: should point to the rabbit-mq instance
- `login` and `password`: should match the rabbit-mq access
  credentials
- `queue`: main queue where tasks are consumed from
- `persistent_manager`: named utility to use to keep tasks state.
- `delay_ttl_ms` and `errored_ttl_ms`: can be used to configure queue
  delays. Default to 2 minutes and 1 week, correspondingly.
- `max_running_tasks`: maximum number of simultaneous asyncio tasks
  hat workers are allowed to run.
- `max_task_retries`: Max number of retries before an errored task is
  sent to the dead letter queue. If set to `None`, it will be retried
  forever.

## Dependencies

Python >= 3.7


## Installation
This example will use virtualenv::

    virtualenv .
    ./bin/pip install .[test]

## Running
Most simple way to get running::

    ./bin/guillotina

## Queue tasks
```python

    from guillotina_amqp import add_task
    await add_task(my_func, 'foobar', kw_arg='blah')
```

## With decorators
```python
from guillotina_amqp import task

    @task
    async def my_func(foo):
        print(foo)

    await my_func('bar')
```

## Run the worker
```bash
    g amqp-worker
```
You can use a couple of additional parameters:

- `--auto-kill-timeout`: time of inactivity after which the worker will restart
  himself assuming it got stuck.
- `--max-running-tasks`: max number of simultaneous asyncio tasks in the event loop.
  Overwrites configuraiton parameter.


## API
- `GET /@amqp-tasks` - get list of tasks
- `GET /@amqp-tasks/{task_id}` - get task info
- `DELETE /@amqp-tasks/{task_id}` - delete task
