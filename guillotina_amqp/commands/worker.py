from aiohttp import web
from guillotina import glogging
from guillotina import task_vars
from guillotina.commands.server import ServerCommand
from guillotina.tests.utils import get_mocked_request
from guillotina_amqp.worker import Worker

import asyncio
import os
import threading


try:
    import prometheus_client
except ImportError:
    prometheus_client = None


logger = glogging.getLogger("guillotina_amqp")


async def prometheus_view(request):
    if prometheus_client is None:
        return None
    output = prometheus_client.exposition.generate_latest()
    return web.Response(text=output.decode("utf8"))


def loop_check(loop, timeout):
    # Elapsed time since last update
    diff = loop.time() - getattr(loop, "__ping_time__", 0)

    if diff > timeout:
        logger.error(f"Exiting worker because no activity in {diff} seconds")
        os._exit(0)
    else:
        # Schedule a check again
        threading.Timer(timeout / 4, loop_check, args=[loop, timeout]).start()
        logger.debug(f"Last refreshed watchdog was {diff}s. ago")


async def loop_probe(loop):
    """This method is used to trigger a context switching in the event
    loop and measure elapsed time between runs (10s)
    """
    while True:
        # Update the watchdog time
        loop.__ping_time__ = loop.time()
        await asyncio.sleep(10)


class WorkerCommand(ServerCommand):
    """Guillotina command to start a worker"""

    description = "AMQP worker"

    def get_parser(self):
        parser = super().get_parser()
        parser.add_argument(
            "--auto-kill-timeout",
            help="How long of no activity before we automatically kill process (in minutes)",
            type=int,
            default=-1,
        )
        parser.add_argument(
            "--max-running-tasks",
            help="Max simultaneous running tasks",
            type=int,
            default=None,
        )
        parser.add_argument(
            "--metrics-server",
            help="Launch an API server to expose metrics",
            default=False,
            action="store_true",
        )
        return parser

    def run(self, arguments, settings, app):
        loop = self.get_loop()
        if arguments.metrics_server:
            asyncio.ensure_future(
                self.run_worker(arguments, settings, app, loop=loop), loop=loop
            )
            port = arguments.port or settings.get("address", settings.get("port"))
            app = web.Application()
            app.router.add_get("/metrics", prometheus_view)
            web.run_app(app, port=port or 8080)
        else:
            loop.run_until_complete(self.run_worker(arguments, settings, app))

    async def run_worker(self, arguments, settings, app, loop=None):
        try:
            await self._run_worker(arguments, settings, app, loop)
        except Exception:
            logger.error("Error running worker. Exiting", exc_info=True)
            os._exit(0)

    async def _run_worker(self, arguments, settings, app, loop=None):
        self.request = get_mocked_request()
        task_vars.request.set(self.request)

        loop = loop or self.get_loop()

        # Run the actual worker in the same loop
        worker = Worker(self.request, loop, arguments.max_running_tasks)
        await worker.start()

        timeout = arguments.auto_kill_timeout
        if timeout > 0:
            loop.create_task(loop_probe(loop))
            # We need to run this outside the main loop and the current thread
            threading.Timer(timeout / 4, loop_check, args=[loop, timeout]).start()

        while True:
            # make this run forever...
            await asyncio.sleep(999999)
