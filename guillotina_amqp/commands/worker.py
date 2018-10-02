from guillotina.commands import Command
from guillotina_amqp.worker import Worker

import aiotask_context
import asyncio
import logging
import sys
import time
import threading
import os


logger = logging.getLogger('guillotina_amqp')
logging.basicConfig(level=logging.INFO)

class EventLoopWatchdog(threading.Thread):
    def __init__(self, loop, timeout):
        super().__init__()
        self.loop = loop
        self.timeout = timeout * 60  # In seconds
        self._time = loop.time()

    def check(self):
        _old_time, self._time = self._time, self.loop.time()
        diff = self._time - _old_time
        if diff > self.timeout:
            logger.error(f'Exiting worker because no activity in {diff} seconds')
            os._exit(1)
        else:
            threading.Timer(self.timeout/2, self.check).start()

    def run(self):
        threading.Timer(10, self.check).start()


# This method just to trigger a context switching in the event loop in
# case nothing is running. Most likely is not even needed since RMQ/Redis
# drivers also are running in the same loop.
async def probe(timeout):
    while True:
        await asyncio.sleep(timeout)

class WorkerCommand(Command):
    description = 'AMQP worker'

    def get_parser(self):
        parser = super().get_parser()
        parser.add_argument('--auto-kill-timeout',
                            help='How long of no activity before we automatically kill process',
                            type=int, default=-1)
        return parser

    async def run(self, arguments, settings, app):
        aiotask_context.set('request', self.request)
        worker = Worker(self.request, self.get_loop())
        await worker.start()
        if arguments.auto_kill_timeout > 0:
            timeout = arguments.auto_kill_timeout
            self.get_loop().create_task(probe(timeout/2))
            t = EventLoopWatchdog(self.get_loop(), timeout)
            t.start()

        while True:
            # make this run forever...
            await asyncio.sleep(999999)
