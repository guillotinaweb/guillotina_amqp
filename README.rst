guillotina_amqp Docs
--------------------

Integrates aioamqp into guillotina.


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
           "heartbeat": 800
	    }
    }


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
