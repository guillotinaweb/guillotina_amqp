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

class EventLoopWatchdog(threading.Thread):
    def __init__(self, loop, timeout):
        super().__init__()
        self.loop = loop
        self.timeout = timeout * 60  # In seconds
        self._time = loop.time()

    def check(self):
        diff = self.loop.time() - self._time

        if diff > self.timeout:
            logger.error(f'Exiting worker because no activity in {diff} seconds')
            os._exit(1)
        else:
            threading.Timer(self.timeout/4, self.check).start()
            logger.debug(f'Last refreshed watchdog was {diff}s. ago')

    # This method just to trigger a context switching in the event loop in
    # case nothing is running. Most likely is not even needed since RMQ/Redis
    # drivers also are running in the same loop.
    async def probe(self):
        while True:
            await asyncio.sleep(10)
            self._time = self.loop.time()

    def run(self):
        self.loop.create_task(self.probe())
        threading.Timer(self.timeout/4, self.check).start()


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
            t = EventLoopWatchdog(self.get_loop(), timeout)
            t.start()

        while True:
            # make this run forever...
            await asyncio.sleep(999999)
