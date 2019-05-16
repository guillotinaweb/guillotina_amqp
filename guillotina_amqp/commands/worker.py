from guillotina.commands import Command
from guillotina.commands.server import ServerCommand
from guillotina_amqp.worker import Worker
from guillotina import glogging
from aiohttp import web

import aiotask_context
import asyncio
import threading
import os


logger = glogging.getLogger('guillotina_amqp')


class EventLoopWatchdog(threading.Thread):
    """Takes care of exiting worker after specified loop no-activity
    timeout for the worker loop.

    This prevents a task from taking over the asynio loop forever and
    preventing other tasks to run.

    If a task hangs, the watchdog will exit the worker and unfinished jobs
    will be taken by other workers.
    """
    def __init__(self, loop, timeout):
        super().__init__()
        self.loop = loop
        self.timeout = timeout * 60  # In seconds
        # Start time
        self._time = loop.time()

    def check(self):
        # Elapsed time since last update
        diff = self.loop.time() - self._time

        if diff > self.timeout:
            logger.error(f'Exiting worker because no activity in {diff} seconds')
            os._exit(1)
        else:
            # Schedule a check again
            threading.Timer(self.timeout/4, self.check).start()
            logger.debug(f'Last refreshed watchdog was {diff}s. ago')

    async def probe(self):
        """This method is used to trigger a context switching in the event
        loop and measure elapsed time between runs (10s)
        """
        while True:
            await asyncio.sleep(10)
            # Update the watchdog time
            self._time = self.loop.time()

    def run(self):
        self.loop.create_task(self.probe())
        threading.Timer(self.timeout/4, self.check).start()


class WorkerCommand(Command):
    """Guillotina command to start a worker"""
    description = 'AMQP worker'

    def get_parser(self):
        parser = super().get_parser()
        parser.add_argument('--auto-kill-timeout',
                            help='How long of no activity before we automatically kill process (in minutes)',
                            type=int, default=-1)
        parser.add_argument('--max-running-tasks',
                            help='Max simultaneous running tasks',
                            type=int, default=None)
        parser.add_argument('--metrics-server', help='Launch an API server to expose metrics',
                            default=False, action='store_true')
        return parser

    def run(self, arguments, settings, app):
        loop = self.get_loop()
        fut = loop.run_in_executor(None, self.run_worker, arguments, settings, app)
        asyncio.ensure_future(fut)

        if arguments.metrics_server:
            try:
                web.run_app(app, port=8080)
            except asyncio.CancelledError:
                # server shut down, we're good here.
                pass

    async def run_worker(self, arguments, settings, app):
        aiotask_context.set('request', self.request)

        # Run the actual worker in the same loop
        worker = Worker(self.request, self.get_loop(),
                        arguments.max_running_tasks)
        await worker.start()

        timeout = arguments.auto_kill_timeout
        if timeout > 0:
            # We need to run this outside the main loop and the current thread
            thread = EventLoopWatchdog(self.get_loop(), timeout)
            thread.start()

        while True:
            # make this run forever...
            await asyncio.sleep(999999)
