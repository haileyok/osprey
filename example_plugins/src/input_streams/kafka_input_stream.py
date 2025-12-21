import json
import platform
from time import time
from typing import Iterator

import sentry_sdk
from kafka.consumer.fetcher import ConsumerRecord
from kafka.consumer.group import RoundRobinPartitionAssignor
from osprey.engine.executor.execution_context import Action
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.utils.dates import parse_go_timestamp
from osprey.worker.sinks.sink.input_stream import BaseInputStream
from osprey.worker.sinks.utils.acking_contexts import (
    BaseAckingContext,
    NoopAckingContext,
)
from osprey.worker.sinks.utils.kafka import PatchedKafkaConsumer
from shared.metrics import prom_metrics

logger = get_logger()


class KafkaInputStream(BaseInputStream[BaseAckingContext[Action]]):
    """An input stream that consumes messages from a Kafka topic and yields Action objects wrapped in an AckingContext."""

    def __init__(self, config: Config):
        super().__init__()

        client_id = config.get_str('OSPREY_KAFKA_INPUT_STREAM_CLIENT_ID', platform.node())
        client_id_suffix = config.get_optional_str('OSPREY_KAFKA_INPUT_STREAM_CLIENT_ID_SUFFIX')
        input_topic: str = config.get_str('OSPREY_KAFKA_INPUT_STREAM_TOPIC', 'osprey.actions_input')
        input_bootstrap_servers: list[str] = config.get_str_list('OSPREY_KAFKA_BOOTSTRAP_SERVERS', ['localhost'])
        group_id = config.get_optional_str('OSPREY_KAFKA_GROUP_ID')

        if client_id_suffix:
            client_id = f'{client_id}-{client_id_suffix}'

        consumer: PatchedKafkaConsumer = PatchedKafkaConsumer(
            input_topic,
            bootstrap_servers=input_bootstrap_servers,
            client_id=client_id,
            group_id=group_id,
            partition_assignment_strategy=(RoundRobinPartitionAssignor,),
        )

        self._consumer: PatchedKafkaConsumer = consumer

    def _gen(self) -> Iterator[BaseAckingContext[Action]]:
        while True:
            start_time = time()
            action_name = ''
            status = 'error'
            try:
                record: ConsumerRecord = next(self._consumer)
                data = json.loads(record.value)
                timestamp = parse_go_timestamp(data['send_time'])
                action_data = data['data']
                action_name = action_data['action_name']

                action = Action(
                    action_id=int(action_data['action_id']),
                    action_name=action_data['action_name'],
                    data=action_data['data'],
                    timestamp=timestamp,
                )
                # Wrap in NoopAckingContext for now, or implement a KafkaAckingContext if needed
                yield NoopAckingContext(action)

                status = 'ok'
            except Exception as e:
                logger.exception('Error while consuming from Kafka')
                sentry_sdk.capture_exception(e)
                continue
            finally:
                prom_metrics.events_processed.labels(action_name=action_name, status=status).inc()
                prom_metrics.event_process_duration.labels(action_name=action_name, status=status).observe(
                    time() - start_time
                )
