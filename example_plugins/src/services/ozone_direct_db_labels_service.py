import copy
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import unquote

from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_shared.labels import EntityLabels, LabelReasons, LabelState, LabelStatus
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.storage.labels import LabelsProvider, LabelsServiceBase
from osprey.worker.lib.storage.postgres import init_from_config, scoped_session
from services.ozone_client import OzoneClient
from services.ozone_label_model import OzoneLabelModel
from sqlalchemy import func, select

logger = get_logger(__name__)


class OzoneDirectDbLabelsService(LabelsServiceBase):
    def __init__(self, config: Config, database: str = 'ozone_db') -> None:
        super().__init__()
        self._database_name = database
        try:
            self._client = OzoneClient.get_instance(config=config)
        except Exception as e:
            logger.error(f'Error creating Ozone labels service: {e}')
            self._client = None

    def initialize(self) -> None:
        init_from_config(self._database_name)
        logger.info(f'Initialized OzoneDirectDbLabelsService with database: {self._database_name}')

    def read_labels(self, entity: EntityT[Any]) -> EntityLabels:
        entity_key = unquote(str(entity.id))

        if not entity_key.startswith('did:'):
            return EntityLabels(labels={})

        now = datetime.now(UTC)

        with scoped_session(database=self._database_name) as session:
            # Subquery: find the most recent event (max id) per label value for this URI
            latest_ids_subquery = (
                select(func.max(OzoneLabelModel.id).label('max_id'))
                .where(OzoneLabelModel.uri == entity_key)
                .group_by(OzoneLabelModel.val)
                .subquery()
            )

            # Main query: fetch the full rows for those latest events
            stmt = select(OzoneLabelModel).where(OzoneLabelModel.id == latest_ids_subquery.c.max_id)
            rows = session.scalars(stmt).all()

        labels: dict[str, LabelState] = {}
        for row in rows:
            # neg=True (or truthy) means label was removed — skip it
            if row.neg:
                continue

            # Check expiration
            if row.exp is not None:
                try:
                    exp_dt = datetime.fromisoformat(row.exp)
                    if exp_dt.tzinfo is None:
                        exp_dt = exp_dt.replace(tzinfo=UTC)
                    if exp_dt < now:
                        continue
                except (ValueError, TypeError):
                    logger.warning(f'Skipping label {row.val} with unparseable exp value: {row.exp}')
                    continue

            labels[row.val] = LabelState(status=LabelStatus.ADDED, reasons=LabelReasons())

        return EntityLabels(labels=labels)

    @contextmanager
    def read_modify_write_labels_atomically(self, entity: EntityT[Any]) -> Generator[EntityLabels, None, None]:
        entity_key = unquote(str(entity.id))

        if not entity_key.startswith('did:'):
            yield EntityLabels(labels={})
            return

        labels = self.read_labels(entity)
        old_labels = copy.deepcopy(labels)

        yield labels

        for val in old_labels.labels:
            if val not in labels.labels:
                self._client.add_or_remove_label(action_id=0, entity_id=entity_key, label=val, neg=True)

        for val in labels.labels:
            if val not in old_labels.labels:
                self._client.add_or_remove_label(action_id=0, entity_id=entity_key, label=val, neg=False)


class OzoneDirectDbLabelsProvider(LabelsProvider):
    def __init__(self, config: Config) -> None:
        service = OzoneDirectDbLabelsService(config=config)
        super().__init__(service)
        try:
            self._cache_ttl_seconds = config.get_int('OSPREY_LABELS_CACHE_TTL_SECONDS', 300)
        except RuntimeError:
            self._cache_ttl_seconds = 300

    def cache_ttl(self) -> timedelta:
        return timedelta(seconds=self._cache_ttl_seconds)
