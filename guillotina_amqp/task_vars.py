from contextvars import ContextVar


amqp_job: ContextVar = ContextVar("g_amqp", default=None)
