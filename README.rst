guillotina_amqp Docs
--------------------

Integrates aioamqp into guillotina, providing an execution framework
for asyncio tasks:

  - Guillotina command to start a worker: `amqp-worker`

  - Workers consume tasks from rabbit-mq through the aioamqp integration

  - Redis state manager implementation to keep a global view of
    running tasks

  - Utilities and endpoints for adding new tasks and for task
    cancellation

Its distributed design - the absence of a central worker manager -
makes it more robust. Task cancelation is signaled over the state
manager, and workers will be responsible for stopping canceled tasks.

A watchdog on the asyncio loop can be launched with the
`auto-kill-timeout` command argument, which will kill the worker if
one of its tasks has captured the loop for too long.

When a task fails, the worker will send it to the delay queue, which
has been configured to re-queue tasks to the main queue after a
certain TTL. Failed tasks are retried a limited amount of times.


Configuration
-------------

Example docs::


    {
        "amqp": {
            "host": "localhost",
            "port": 5673,
            "login": "guest",
            "password": "guest",
            "vhost": "/",
            "heartbeat": 800,
            "queue": "guillotina",  # Main consuming queue for workers
            "persistent_manager": "redis",
            "delayed_ttl_ms": 60 * 1000,
            "errored_ttl_ms": 1000 * 60 * 60 * 24 * 7,
        }
    }

- `host` and `port`: should point to the rabbit-mq instance
- `login` and `password`: should match the rabbit-mq access credentials
- `queue`: main queue where tasks are consumed from
- `persistent_manager`: named utility to use to keep tasks state.
- `delay_ttl_ms` and `errored_ttl_ms`: can be used to configure queue delays. Default to 2 minutes and 1 week, correspondingly.
- `max_running_tasks`: maximum number of simultaneous asyncio tasks
  hat workers are allowed to run.

Dependencies
------------

Python >= 3.6


Installation
------------

This example will use virtualenv::


    virtualenv .
    ./bin/pip install .[test]


Running
-------

Most simple way to get running::

    ./bin/guillotina


Queue tasks
-----------

code::

    from guillotina_amqp import add_task
    await add_task(my_func, 'foobar', kw_arg='blah')


With decorators
---------------

code::

    from guillotina_amqp import task

    @task
    async def my_func(foo):
        print(foo)

    await my_func('bar')


Run the worker
--------------

command::

    g amqp-worker

You can use a couple of additional parameters:

- `--auto-kill-timeout`: time of inactivity after which the worker will restart
  himself assuming it got stuck.
- `--max-running-tasks`: max number of simultaneous asyncio tasks in the event loop.
  Overwrites configuraiton parameter.


Beacons
-------

The aioamqp client can get stuck on a closed connection and the worker
would be running forever without processing any message.

As a workaround, we implemented a beacon system independent for every
worker. When getting a new connection to rabbitmq, we create a beacon
queue and a beacon-delay queue. Both are exclusive queues, which means
that will be removed after the connection from the worker is closed.

On the background of every connection, we publish a beacon message to
the beacon-delay queue, which is expected to be read again after a
certain TTL from the beacon queue. If a beacon message is not received
after 3 times the TTL, the worker will exit.


API
---

- `GET /@amqp-tasks` - get list of tasks
- `GET /@amqp-tasks/{task_id}` - get task info
- `DELETE /@amqp-tasks/{task_id}` - delete task