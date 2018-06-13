from guillotina.commands import Command
from guillotina_amqp.worker import Worker

import aiotask_context
import asyncio
import logging
import sys
import time


logger = logging.getLogger('guillotina_amqp')


async def activity_check(worker, timeout):
    while True:
        await asyncio.sleep(60)
        if (time.time() - worker.last_activity) > timeout:
            logger.error(f'Exiting worker because no activity in {timeout} seconds')
            sys.exit(1)


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
            asyncio.ensure_future(activity_check(worker, arguments.auto_kill_timeout))
        while True:
            # make this run forever...
            await asyncio.sleep(999999)
