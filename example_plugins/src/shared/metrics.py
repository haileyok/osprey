import logging

from prometheus_client import Counter, Histogram, start_http_server

NAMESPACE = 'osprey'

logger = logging.getLogger(__name__)


class PromMetrics:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.events_processed = Counter(
            name='events_received',
            namespace=NAMESPACE,
            documentation='Number of requests received',
            labelnames=['action_name', 'status'],
        )

        self.labels_emitted = Counter(
            name='labels_emitted',
            namespace=NAMESPACE,
            documentation='Number of labels emitted to Ozone',
            labelnames=['label', 'status'],
        )

        self.event_process_duration = Histogram(
            name='event_process_duration_seconds',
            namespace=NAMESPACE,
            buckets=(
                0.001,
                0.005,
                0.01,
                0.025,
                0.05,
                0.1,
                0.25,
                0.5,
                1.0,
                2.5,
                5.0,
                10.0,
            ),
            labelnames=['action_name', 'status'],
            documentation='Time taken to process an event',
        )

        self.clickhouse_inserts = Counter(
            name='clickhouse_inserts',
            namespace=NAMESPACE,
            documentation='Number of items inserted into Clickhouse',
            labelnames=['status'],
        )

        self.clickhouse_insert_duration = Histogram(
            name='clickhouse_insert_duration_seconds',
            namespace=NAMESPACE,
            buckets=(
                0.001,
                0.005,
                0.01,
                0.025,
                0.05,
                0.1,
                0.25,
                0.5,
                1.0,
                2.5,
                5.0,
                10.0,
            ),
            labelnames=['status'],
            documentation='Time taken to insert to Clickhouse',
        )

        self._initialized = True

    def start_http(self, port: int, addr: str = '0.0.0.0'):
        logger.info(f'Starting Prometheus client on {addr}:{port}')
        start_http_server(port=port, addr=addr)
        logger.info(f'Prometheus client running on {addr}:{port}')


prom_metrics = PromMetrics()
