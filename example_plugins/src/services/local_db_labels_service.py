import copy
import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator, Sequence

from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_shared.labels import (
    EntityLabels,
    LabelReason,
    LabelReasons,
    LabelState,
    LabelStatus,
)
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.storage.labels import LabelsServiceBase
from osprey.worker.lib.storage.postgres import Model, init_from_config, scoped_session
from result import Err, Ok, Result
from services.ozone_client import OzoneClient
from sqlalchemy import Column, String, DateTime, delete, select, text
from sqlalchemy.dialects.postgresql import insert
from urllib.parse import unquote

logger = get_logger(__name__)


def _row_to_label_state(row: 'LabelsModel') -> LabelState:
    """Convert a LabelsModel row to a LabelState object."""
    return LabelState(
        status=LabelStatus.ADDED,
        reasons=LabelReasons({
            'label-consumer': LabelReason(
                description=f'from labeler {row.src}',
                created_at=row.cts if row.cts else datetime.now(timezone.utc),
            )
        }),
    )


class LabelsModel(Model):
    """SQLAlchemy model for the flat labels table populated by label-consumer."""

    __tablename__ = 'labels'

    src = Column(String, primary_key=True)
    uri = Column(String, primary_key=True)
    val = Column(String, primary_key=True)
    cid = Column(String, nullable=True)
    cts = Column(DateTime(timezone=True), nullable=True)
    exp = Column(DateTime(timezone=True), nullable=True)


class LocalDbLabelsService(LabelsServiceBase):
    """
    Labels service backed by the local flat `labels` table.

    The Go label-consumer populates this table by tailing subscribeLabels.
    This service reads from it, translating rows into EntityLabels objects
    that the HasLabel UDF can consume.
    """

    def __init__(self, config: Config) -> None:
        super().__init__()
        self._database_name: str = 'osprey_db'
        self._ozone_client = None

        try:
            labeler_dids_str = config.get_str(
                'OSPREY_LABELER_DIDS',
                '["did:plc:e4elbtctnfqocyfcml6h2lf7"]',
            )
            self._labeler_dids: list[str] = json.loads(labeler_dids_str)
            self._ozone_client = OzoneClient.get_instance(config=config)
            self._labeler_did: str = config.get_str('OSPREY_BLUESKY_LABELER_DID', '')
        except Exception as e:
            self._labeler_dids = []
            self._labeler_did = ''
            logger.error(f'Failed to initialize LocalDbLabelsService: {e}')

    def initialize(self) -> None:
        init_from_config(self._database_name)
        logger.info('Initialized LocalDbLabelsService')

    def read_labels(self, entity: EntityT[Any]) -> EntityLabels:
        entity_key = str(entity.id)
        entity_key = unquote(entity_key)

        with scoped_session(database=self._database_name) as session:
            stmt = (
                select(LabelsModel)
                .where(LabelsModel.uri == entity_key)
                .where(LabelsModel.src.in_(self._labeler_dids))
                .where(
                    (LabelsModel.exp.is_(None)) | (LabelsModel.exp > text("now()"))
                )
            )
            rows = session.scalars(stmt).all()

            if not rows:
                return EntityLabels()

            labels: dict[str, LabelState] = {}
            for row in rows:
                labels[row.val] = _row_to_label_state(row)

            return EntityLabels(labels=labels)

    def batch_read_labels(self, entities: Sequence[EntityT[Any]]) -> Sequence[Result[EntityLabels, Exception]]:
        if not entities:
            return []

        entity_uris = [unquote(str(entity.id)) for entity in entities]
        results_by_uri: dict[str, EntityLabels] = {uri: EntityLabels() for uri in entity_uris}

        try:
            with scoped_session(database=self._database_name) as session:
                stmt = (
                    select(LabelsModel)
                    .where(LabelsModel.uri.in_(entity_uris))
                    .where(LabelsModel.src.in_(self._labeler_dids))
                    .where(
                        (LabelsModel.exp.is_(None)) | (LabelsModel.exp > text("now()"))
                    )
                )
                rows = session.scalars(stmt).all()

                for row in rows:
                    if row.uri not in results_by_uri:
                        results_by_uri[row.uri] = EntityLabels()
                    results_by_uri[row.uri].labels[row.val] = _row_to_label_state(row)

            return [Ok(results_by_uri[uri]) for uri in entity_uris]
        except Exception as e:
            return [Err(e) for _ in entities]

    @contextmanager
    def read_modify_write_labels_atomically(self, entity: EntityT[Any]) -> Generator[EntityLabels, None, None]:
        """
        Atomic read-modify-write with SELECT FOR UPDATE locking.

        1. Opens a transaction with SELECT FOR UPDATE on the entity's label rows
        2. Yields EntityLabels for in-place mutation by LabelsProvider
        3. Computes delta (old vs new), applies to Ozone AND local table
        4. Commits the transaction

        Follows the locking contract from LabelsServiceBase and the
        delta-comparison pattern from OzoneLabelsService.
        """
        entity_key = str(entity.id)
        entity_key = unquote(entity_key)

        with scoped_session(commit=False, database=self._database_name) as session:
            try:
                stmt = (
                    select(LabelsModel)
                    .where(LabelsModel.uri == entity_key)
                    .where(LabelsModel.src.in_(self._labeler_dids))
                    .where(
                        (LabelsModel.exp.is_(None)) | (LabelsModel.exp > text("now()"))
                    )
                    .with_for_update()
                )
                rows = session.scalars(stmt).all()

                labels_dict: dict[str, LabelState] = {}
                for row in rows:
                    labels_dict[row.val] = _row_to_label_state(row)
                labels = EntityLabels(labels=labels_dict)
                old_labels = copy.deepcopy(labels)

                yield labels

                # Compute delta and apply to Ozone + local table
                for val in old_labels.labels:
                    if val not in labels.labels:
                        # Label removed — negate in Ozone, delete from local table
                        if self._ozone_client:
                            try:
                                self._ozone_client.add_or_remove_label(
                                    action_id=0, entity_id=entity_key, label=val, neg=True,
                                )
                            except Exception as e:
                                logger.error(f'Failed to remove label {val} from Ozone for {entity_key}: {e}')

                        # Write-through: delete from local table
                        if self._labeler_did:
                            stmt_del = delete(LabelsModel).where(
                                LabelsModel.src == self._labeler_did,
                                LabelsModel.uri == entity_key,
                                LabelsModel.val == val,
                            )
                            session.execute(stmt_del)

                for val in labels.labels:
                    if val in old_labels.labels:
                        continue
                    # Label added — add in Ozone, upsert to local table
                    if self._ozone_client:
                        try:
                            self._ozone_client.add_or_remove_label(
                                action_id=0, entity_id=entity_key, label=val, neg=False,
                            )
                        except Exception as e:
                            logger.error(f'Failed to add label {val} to Ozone for {entity_key}: {e}')

                    # Write-through: upsert to local table
                    if self._labeler_did:
                        stmt_ups = insert(LabelsModel).values(
                            src=self._labeler_did,
                            uri=entity_key,
                            val=val,
                            cts=datetime.now(timezone.utc),
                        )
                        stmt_ups = stmt_ups.on_conflict_do_update(
                            index_elements=['src', 'uri', 'val'],
                            set_={'cts': stmt_ups.excluded.cts},
                        )
                        session.execute(stmt_ups)

                session.commit()

            except Exception:
                session.rollback()
                logger.error(f'Rolled back atomic read-modify-write for entity {entity_key}')
                raise
