from guillotina import metrics
from guillotina_amqp.exceptions import ObjectNotFoundException


try:
    from prometheus_client.utils import INF

    import prometheus_client

    RMQ_OPS = prometheus_client.Counter(
        "guillotina_amqp_server_ops_total",
        "Total count of ops by type of operation and the error if there was.",
        labelnames=["type", "error"],
    )
    RMQ_OPS_PROCESSING_TIME = prometheus_client.Histogram(
        "guillotina_amqp_server_ops_processing_time_seconds",
        "Histogram of operations processing time by type (in seconds)",
        labelnames=["type"],
    )

    JOB = prometheus_client.Counter(
        "guillotina_amqp_job_ops_total",
        "Total count of ops by type of operation and the error if there was.",
        labelnames=["type", "error"],
    )
    JOB_PROCESSING_TIME = prometheus_client.Histogram(
        "guillotina_amqp_job_ops_processing_time_seconds",
        "Histogram of operations processing time by type (in seconds)",
        labelnames=["type"],
        buckets=(0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, INF),
    )

    JOB_REQUEST = prometheus_client.Counter(
        "guillotina_amqp_job_request_ops_total",
        "Total count of request creation ops and the error if there was.",
        labelnames=["error"],
    )

    JOB_REQUEST_TIME = prometheus_client.Histogram(
        "guillotina_amqp_job_request_ops_time_seconds",
        "Histogram of requuest creation op time (in seconds)",
        buckets=(0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, INF),
    )

    JOB_COMMIT = prometheus_client.Counter(
        "guillotina_amqp_job_commit_ops_total",
        "Total count of job commit ops and the error if there was.",
        labelnames=["type", "error"],
    )

    JOB_COMMIT_TIME = prometheus_client.Histogram(
        "guillotina_amqp_job_commit_ops_time_seconds",
        "Histogram of job commit ops time (in seconds)",
        labelnames=["type"],
        buckets=(0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, INF),
    )

    class watch_amqp(metrics.watch):
        def __init__(self, operation: str):
            super().__init__(
                counter=RMQ_OPS,
                histogram=RMQ_OPS_PROCESSING_TIME,
                labels={"type": operation},
            )

    class watch_job(metrics.watch):
        def __init__(self, operation: str):
            super().__init__(
                counter=JOB,
                histogram=JOB_PROCESSING_TIME,
                labels={"type": operation},
                error_mappings={"notfound": ObjectNotFoundException},
            )

    watch_job_request = metrics.watch(counter=JOB_REQUEST, histogram=JOB_REQUEST_TIME,)

    class watch_job_commit(metrics.watch):
        def __init__(self, operation: str):
            super().__init__(
                counter=JOB_COMMIT,
                histogram=JOB_COMMIT_TIME,
                labels={"type": operation},
            )


except ImportError:
    watch_job = watch_amqp = watch_job_request = watch_job_commit = metrics.dummy_watch  # type: ignore
