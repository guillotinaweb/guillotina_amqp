import logging
from guillotina_amqp.job import logger as logger_job
from guillotina_amqp.worker import logger as logger_worker

logging.basicConfig()
logger_job.setLevel(10)
logger_worker.setLevel(10)
