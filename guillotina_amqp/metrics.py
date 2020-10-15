from guillotina import metrics


try:
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

    class watch_amqp(metrics.watch):
        def __init__(self, operation: str):
            super().__init__(
                counter=RMQ_OPS,
                histogram=RMQ_OPS_PROCESSING_TIME,
                labels={"type": operation},
            )


except ImportError:
    watch_amqp = metrics.dummy_watch  # type: ignore
