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
    # and sense the delta between the ideal timeout (10s)
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
        timeout = arguments.auto_kill_timeout
        aiotask_context.set('request', self.request)
        worker = Worker(self.request, self.get_loop())
        await worker.start()
        if timeout > 0:
            # We need to run this outside the main loop and the current thread
            thread = EventLoopWatchdog(self.get_loop(), timeout)
            thread.start()

        while True:
            # make this run forever...
            await asyncio.sleep(999999)
