from datetime import datetime
from time import time
from typing import Any, Dict, List, Set

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from gevent.lock import RLock
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.sinks.sink.output_sink import BaseOutputSink
from shared.metrics import prom_metrics

logger = get_logger('clickhouse_execution_results_sink')

# define a batch size, this should likely be configurable or even adjusted automatically based on
# rate of events
DEFAULT_BATCH_SIZE = 1000


class ClickhouseExecutionResultsSink(BaseOutputSink):
    def __init__(self, config: Config):
        ch_host = config.get_str('OSPREY_CLICKHOUSE_HOST', 'localhost')
        ch_port = config.get_int('OSPREY_CLICKHOUSE_PORT', 8123)
        ch_user = config.get_str('OSPREY_CLICKHOUSE_USER', 'default')
        ch_pass = config.get_str('OSPREY_CLICKHOUSE_PASSWORD', 'clickhouse')
        ch_db = config.get_str('OSPREY_CLICKHOUSE_DB', 'default')
        ch_table = config.get_str('OSPREY_CLICKHOUSE_TABLE', 'osprey_execution_results')

        # batch size needs to be adjusted based on throughput. consider a batch size that is some 3-4x your
        # peak throughput rate, since clickhouse does get upset if you attempt to insert too small of batches
        # at too fast of a rate. note that batch sizes may be considerably large.
        ch_batch_size = config.get_int('OSPREY_CLICKHOUSE_BATCH_SIZE', DEFAULT_BATCH_SIZE)

        self._ch_database = ch_db
        self._ch_table = ch_table
        self._ch_batch_size = ch_batch_size

        self._ch_client: Client = clickhouse_connect.get_client(
            host=ch_host,
            port=ch_port,
            username=ch_user,
            password=ch_pass,
        )

        self._batch: List[Dict[str, Any]] = []
        self._batch_lock = RLock()
        self._schema_lock = RLock()

        self._known_columns: Set[str] = self._get_schema()
        logger.info(f'Initialized Clickhouse sink with {len(self._known_columns)} known columns in the schema')

    def will_do_work(self, result: ExecutionResult) -> bool:
        return True

    def push(self, result: ExecutionResult) -> None:
        """Add result to batch and insert when batch is full"""
        row: Dict[str, Any] = {}
        row.update(result.extracted_features)

        batch_to_flush = None
        with self._batch_lock:
            self._batch.append(row)
            if len(self._batch) >= self._ch_batch_size:
                batch_to_flush = self._batch.copy()
                self._batch = []

        if batch_to_flush:
            self._flush_batch(batch_to_flush)

    def _flush_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Insert a batch into Clickhouse"""

        status = 'error'
        start_time = time()

        all_fields: Set[str] = set()
        for row in batch:
            all_fields.update(row.keys())

        new_fields = all_fields - self._known_columns
        if new_fields:
            try:
                with self._schema_lock:
                    new_fields = all_fields - self._known_columns
                    if new_fields:
                        self._add_columns(new_fields, batch)
                        self._known_columns.update(new_fields)
            except Exception as e:
                logger.error(f'Error updating the Clickhouse table schema: {e}')
                with self._batch_lock:
                    self._batch.extend(batch)
                raise

        col_names: List[str] = sorted(self._known_columns)

        data_rows: List[List[Any]] = []
        for row in batch:
            data_row = []
            for col in col_names:
                val = row.get(col)
                if val is None and col in self._array_columns:
                    val = []
                data_row.append(val)
            data_rows.append(data_row)

        try:
            self._ch_client.insert(
                table=f'{self._ch_database}.{self._ch_table}', data=data_rows, column_names=col_names
            )
            logger.info(f'Inserted {len(batch)} rows into ClickHouse')
        except Exception as e:
            logger.error(f'Error flushing batch to Clickhouse: {e}')
            with self._batch_lock:
                self._batch.extend(batch)
            raise
        finally:
            prom_metrics.clickhouse_inserts.labels(status=status).inc(amount=len(batch))
            prom_metrics.clickhouse_insert_duration.labels(status=status).observe(time() - start_time)

    def _get_schema(self) -> Set[str]:
        """Get the current schema of the Clickhouse table"""
        try:
            result = self._ch_client.query(f'DESCRIBE TABLE {self._ch_database}.{self._ch_table}')
            self._array_columns: Set[str] = {row[0] for row in result.result_rows if 'Array' in row[1]}
            return {row[0] for row in result.result_rows}
        except Exception as e:
            logger.warning(f'Could not get current schema: {e}')
            self._array_columns = set()
            return set()

    def _add_columns(self, new_fields: Set[str], batch_sample: List[Dict[str, Any]]) -> None:
        """Add new columns to the Clickhouse table"""
        if not new_fields:
            return

        alter_statements: List[str] = []
        for field_name in new_fields:
            column_type = self._infer_column_type(field_name, batch_sample)
            alter_statements.append(f'ADD COLUMN IF NOT EXISTS `{field_name}` {column_type}')

            if column_type.startswith('Array'):
                self._array_columns.add(field_name)

        alter_query = f"""
            ALTER TABLE {self._ch_database}.{self._ch_table}
            {', '.join(alter_statements)}
        """

        try:
            self._ch_client.command(alter_query)
            logger.info(f'Added columns: {", ".join(new_fields)}')
        except Exception as e:
            logger.error(f'Error adding columns {new_fields}: {e}')
            raise

    def _infer_column_type(self, field_name: str, batch_sample: List[Dict[str, Any]]) -> str:
        """Infer Clickhouse column type from sample data"""
        sample_value = None
        for row in batch_sample:
            if field_name in row and row[field_name] is not None:
                sample_value = row[field_name]
                break

        if sample_value is None:
            return 'Nullable(String)'

        if isinstance(sample_value, bool):
            return 'Nullable(UInt8)'
        elif isinstance(sample_value, int):
            return 'Nullable(Int64)'
        elif isinstance(sample_value, float):
            return 'Nullable(Float64)'
        elif isinstance(sample_value, datetime):
            return 'Nullable(DateTime64(3))'
        elif isinstance(sample_value, list):
            return 'Array(String)'
        else:
            return 'Nullable(String)'

    def stop(self) -> None:
        """Flush any remaining data on shutdown"""
        logger.info('Stopping Clickhouse sink, flushing remaining batch...')
        self._flush_batch()
