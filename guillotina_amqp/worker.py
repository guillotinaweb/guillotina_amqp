from .metrics import watch_amqp
from guillotina import app_settings
from guillotina import glogging
from guillotina_amqp import amqp
from guillotina_amqp.job import Job
from guillotina_amqp.state import get_state_manager
from guillotina_amqp.state import TaskState
from guillotina_amqp.state import TaskStatus
from guillotina_amqp.state import update_task_canceled
from guillotina_amqp.state import update_task_errored
from guillotina_amqp.state import update_task_finished
from guillotina_amqp.state import update_task_scheduled

import asyncio
import guillotina_amqp
import json
import os
import time


try:
    import prometheus_client

    OPS = prometheus_client.Counter(
        "guillotina_amqp_worker_ops_total",
        "Total count of ops by type of operation and the error if there was.",
        labelnames=["type", "status"],
    )

    def record_op_metric(type: str, status: str) -> None:
        OPS.labels(type=type, status=status).inc()


except ImportError:

    def record_op_metric(type: str, status: str) -> None:
        ...


logger = glogging.getLogger("guillotina_amqp.worker")
default_delayed = 1000 * 60 * 2  # 2 minutes
default_errored = 1000 * 60 * 60 * 24 * 7 * 1  # 1 week


class Worker:
    """Workers hold an asyncio loop in which will run several tasks. It
    reads from RabbitMQ for new job descriptions and will run them in
    asyncio tasks.

    The worker is aware of a state manager in which he posts job
    results.

    """

    sleep_interval = 0.1
    last_activity = time.time()
    update_status_interval = 20
    total_run = 0
    total_errored = 0
    _status_task = None
    _activity_task = None

    def __init__(self, request=None, loop=None, max_size=None, check_activity=True):
        self.request = request
        self.loop = loop
        self._running = []
        self._max_running = int(
            max_size or app_settings["amqp"].get("max_running_tasks", 5)
        )
        self.max_task_retries = app_settings["amqp"].get("max_task_retries", None)
        self._closing = False
        self._state_manager = None
        self._state_ttl = int(app_settings["amqp"]["state_ttl"])
        self._check_activity = check_activity

        # RabbitMQ queue names defined here
        self.MAIN_EXCHANGE = app_settings["amqp"]["exchange"]
        self.QUEUE_MAIN = app_settings["amqp"]["queue"]
        self.QUEUE_ERRORED = app_settings["amqp"]["queue"] + "-error"
        self.QUEUE_DELAYED = app_settings["amqp"]["queue"] + "-delay"
        self.TTL_ERRORED = app_settings["amqp"].get("errored_ttl_ms", default_errored)
        self.TTL_DELAYED = app_settings["amqp"].get("delayed_ttl_ms", default_delayed)

    @property
    def state_manager(self):
        if self._state_manager is None:
            self._state_manager = get_state_manager()
        return self._state_manager

    @property
    def num_running(self):
        """Returns the number of currently running jobs"""
        return len(self._running)

    async def handle_queued_job(self, channel, body, envelope, properties):
        """Callback triggered when there is a new job in the job channel (e.g:
        a new task in a rabbitmq queue)

        """
        logger.debug(f"Queued job {body}")

        # Deserialize job description
        if not isinstance(body, str):
            body = body.decode("utf-8")
        data = json.loads(body)

        # Set status to scheduled
        task_id = data["task_id"]
        dotted_name = data["func"]

        await update_task_scheduled(self.state_manager, task_id, eventlog=[])

        logger.info(f"Received task: {task_id}: {dotted_name}")

        # Block if we reached maximum number of running tasks
        while len(self._running) >= self._max_running:
            logger.info(f"Max running tasks reached: {self._max_running}")
            await asyncio.sleep(self.sleep_interval)
            self.last_activity = time.time()

        # Create job object
        self.last_activity = time.time()
        job = Job(self.request, data, channel, envelope)
        # Get the redis lock on the task so no other worker takes it
        _id = job.data["task_id"]
        ts = TaskState(_id)

        # Cancelation
        if await ts.is_canceled():
            record_op_metric(job.function_name, TaskStatus.CANCELED)
            logger.warning(f"Task {_id} has already been canceled")
            # Ack so that canceled job is removed from main queue
            with watch_amqp("ack"):
                await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)
            return

        if not await ts.acquire():
            record_op_metric(job.function_name, "alreadyrunning")
            logger.warning(f"Task {_id} is already running in another worker")
            with watch_amqp("ack"):
                await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)
            return

        # Record job's data into global state
        await self.state_manager.update(task_id, {"job_data": job.data})

        # Add the task to the loop and start it
        task = self.loop.create_task(job())

        record_op_metric(job.function_name, TaskStatus.RUNNING)

        task._job = job
        job.task = task
        self._running.append(task)
        task.add_done_callback(self._task_done_callback)

    async def _handle_canceled(self, task):
        task_id = task._job.data["task_id"]
        # ACK to main queue to it is not scheduled anymore
        with watch_amqp("ack"):
            await task._job.channel.basic_client_ack(
                delivery_tag=task._job.envelope.delivery_tag
            )

        # Set status to cancelled
        await update_task_canceled(
            self.state_manager, task_id, task=task, ttl=self._state_ttl
        )

        record_op_metric(task._job.function_name, TaskStatus.CANCELED)

    async def _handle_max_retries_reached(self, task):
        task_id = task._job.data["task_id"]
        logger.warning(f"Task {task_id} reached max {self.max_task_retries} retries")

        # Send NACK, as we exceeded the number of retrials
        with watch_amqp("nack"):
            await task._job.channel.basic_client_nack(
                delivery_tag=task._job.envelope.delivery_tag,
                multiple=False,
                requeue=False,
            )

        # Update status to errored with the traceback
        await update_task_errored(
            self.state_manager, task_id, task=task, ttl=self._state_ttl
        )

        record_op_metric(task._job.function_name, TaskStatus.ERRORED)

    async def _handle_retry(self, task, current_retries):
        task_id = task._job.data["task_id"]
        channel = task._job.channel
        logger.debug(f"handle_retry: task {task_id} retries {current_retries}")
        # Increment retry count
        await update_task_errored(
            self.state_manager,
            task_id,
            task=task,
            ttl=self._state_ttl,
            job_retries=current_retries + 1,
        )

        # Publish task data to delay queue
        with watch_amqp("publish"):
            await channel.publish(
                json.dumps(task._job.data),
                exchange_name=self.MAIN_EXCHANGE,
                routing_key=self.QUEUE_DELAYED,
                properties={"delivery_mode": 2},
            )
        # ACK to main queue so it doesn't timeout
        with watch_amqp("ack"):
            await channel.basic_client_ack(delivery_tag=task._job.envelope.delivery_tag)
        logger.info(f"Task {task_id} will be retried")

        record_op_metric(task._job.function_name, "retried")

    async def _handle_successful(self, task):
        task_id = task._job.data["task_id"]
        dotted_name = task._job.data["func"]

        # ACK rabbitmq: will make task disappear from rabbitmq
        with watch_amqp("ack"):
            await task._job.channel.basic_client_ack(
                delivery_tag=task._job.envelope.delivery_tag
            )

        # Update status with result
        await update_task_finished(
            self.state_manager,
            task_id,
            task=task,
            ttl=self._state_ttl,
            result=task.result(),
        )
        logger.info(f"Finished task: {task_id}: {dotted_name}")

        record_op_metric(task._job.function_name, TaskStatus.FINISHED)

    def _task_done_callback(self, task):
        # We can't pass coroutines to add_done_callback so we have to
        # place it inside an ensure_future
        asyncio.ensure_future(self._task_callback(task))

    async def _task_callback(self, task):
        """This is called when a job finishes execution"""
        task_id = task._job.data["task_id"]
        self.total_run += 1

        try:
            result = task.result()
            logger.debug(f"Task data: {task._job.data}, result: {result}")
        except asyncio.CancelledError:
            logger.warning(f"Task got cancelled: {task._job.data}")
            return await self._handle_canceled(task)
        except Exception:
            logger.error(f"Unhandled task exception: {task_id}", exc_info=True)
            # Error during execution of the task
            #
            # If max retries reached
            existing_data = await self.state_manager.get(task_id)
            retrials = existing_data.get("job_retries", 0)

            if self.max_task_retries is not None and retrials >= self.max_task_retries:
                return await self._handle_max_retries_reached(task)

            # Otherwise let task be retried
            return await self._handle_retry(task, retrials)
        else:
            # If task ran successfully, ACK main queue and finish
            return await self._handle_successful(task)
        finally:
            if task in self._running:
                self._running.remove(task)
            await self.state_manager.release(task_id)

    async def stop(self):
        self.cancel()
        await amqp.remove_connection()

    async def start(self):
        """Called on worker startup. Connects to the rabbitmq. Declares and
        configures the different queues.

        """
        channel, transport, protocol = await amqp.get_connection()

        # Declare main exchange
        await channel.exchange_declare(
            exchange_name=self.MAIN_EXCHANGE, type_name="direct", durable=True
        )

        # Declare errored queue and bind it
        await self.queue_errored(channel, passive=False)

        # Declare main queue and bind it
        await self.queue_main(channel, passive=False)

        # Declare delayed queue and bind it
        await self.queue_delayed(channel, passive=False)

        await channel.basic_qos(prefetch_count=self._max_running)

        # Configure task consume callback
        await channel.basic_consume(self.handle_queued_job, queue_name=self.QUEUE_MAIN)

        # Start task that will update status periodically
        self._status_task = asyncio.ensure_future(self.update_status())

        # Start task that checks connection activity
        self._activity_task = asyncio.ensure_future(self.check_activity())

        logger.warning(f"Subscribed to queue: {self.QUEUE_MAIN}")

    async def queue_main(self, channel, passive=True):
        """Declares the main queue for task messages. NACKed messages are sent
        to the errored queue.

        If passie is False, will additionally bind the queue to the
        exchange
        """
        resp = await channel.queue_declare(
            queue_name=self.QUEUE_MAIN,
            durable=True,
            passive=passive,
            arguments={
                "x-dead-letter-exchange": self.MAIN_EXCHANGE,
                "x-dead-letter-routing-key": self.QUEUE_ERRORED,
            },
        )
        if not passive:
            await channel.queue_bind(
                exchange_name=self.MAIN_EXCHANGE,
                queue_name=self.QUEUE_MAIN,
                routing_key=self.QUEUE_MAIN,
            )
        return resp

    async def queue_delayed(self, channel, passive=True):
        """Declares the queue for delayed tasks, which is used for failed
        tasks retrials. After self.TTL_DELAYED, tasks will be requeued
        to the main task queue.
        """
        resp = await channel.queue_declare(
            queue_name=self.QUEUE_DELAYED,
            durable=True,
            passive=passive,
            arguments={
                "x-dead-letter-exchange": self.MAIN_EXCHANGE,
                "x-dead-letter-routing-key": self.QUEUE_MAIN,
                "x-message-ttl": self.TTL_DELAYED,
            },
        )
        if not passive:
            await channel.queue_bind(
                exchange_name=self.MAIN_EXCHANGE,
                queue_name=self.QUEUE_DELAYED,
                routing_key=self.QUEUE_DELAYED,
            )
        return resp

    async def queue_errored(self, channel, passive=True):
        """Declares queue for errored tasks. Errored tasks will remain a
        limited period of time and then they will be lost.
        """
        resp = await channel.queue_declare(
            queue_name=self.QUEUE_ERRORED,
            durable=True,
            passive=passive,
            arguments={"x-message-ttl": self.TTL_ERRORED},
        )
        if not passive:
            await channel.queue_bind(
                exchange_name=self.MAIN_EXCHANGE,
                queue_name=self.QUEUE_ERRORED,
                routing_key=self.QUEUE_ERRORED,
            )
        return resp

    def cancel(self):
        """
        Cancels the worker (i.e: all its running tasks)
        """
        for task in self._running[:]:
            if not task.done():
                task.cancel()
            self._running.remove(task)

        for task in (self._status_task, self._activity_task):
            if task is not None and not task.done():
                task.cancel()

    async def join(self):
        """
        Waits for all tasks to finish
        """
        while len(self._running) > 0:
            await asyncio.sleep(0.01)

    async def check_activity(self, check_every=30, noop_after=200, kill_after=300):
        """Makes sure there's always activity in the connection by scheduling
        NOOP tasks if no activity after more than 30 seconds.

        It will kill the worker if no activity detected in the last 5
        minutes, which would most likely mean that the AMQP connection
        got stalled. This assumes deployments to ensure enough
        replicas for worker instances, and that workers will be
        restarted in case of being killed.

        """
        if not self._check_activity:
            # Nothing to do
            return

        while True:
            await asyncio.sleep(check_every)

            if len(self._running) != 0:
                # Tasks are running, so check again later
                continue

            # Kill worker if no activity in the last 5 minutes
            diff = time.time() - self.last_activity
            if diff > kill_after:
                logger.error(
                    f"Exiting worker because no connection activity in {diff} seconds"
                )
                os._exit(0)

            # Send NOOP tasks if no activity in the last 30 seconds
            if diff > noop_after:
                try:
                    await _noop()
                except Exception:
                    logger.error("Error scheduling NOOP task", exc_info=True)
                    pass

    async def update_status(self):
        """Updates status for running tasks and kills running tasks that have
        been canceled.

        """
        while True:
            await asyncio.sleep(self.update_status_interval)

            for task in self._running:
                _id = task._job.data["task_id"]
                ts = TaskState(_id)

                # Still working on the job: refresh task lock
                await ts.refresh_lock()

            # Cancel local tasks that have been cancelled in global
            # state manager
            async for val in self.state_manager.cancelation_list():
                for task in self._running:
                    _id = task._job.data["task_id"]
                    if _id == val:
                        logger.warning(f"Canceling task {_id}")
                        if not task.done():
                            task.cancel()
                        self._running.remove(task)
                        await self._state_manager.clean_canceled(_id)


@guillotina_amqp.task
async def _noop():
    """
    Does nothing at all
    """
    logger.debug("NOOP task executed")
    return
