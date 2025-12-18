import copy
from contextlib import contextmanager
from typing import Any, Dict, Generator
from urllib.parse import unquote

from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_shared.labels import EntityLabels, LabelState
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.storage.labels import LabelsServiceBase
from services.ozone_client import OzoneClient

logger = get_logger('ozone_labels_service')


class OzoneLabelsService(LabelsServiceBase):
    def __init__(self, config: Config):
        super().__init__()
        try:
            self._client = OzoneClient.get_instance(config=config)
        except Exception as e:
            logger.error(f'Error creating Ozone labels service: {e}')

    def initialize(self) -> None:
        pass

    def read_labels(self, entity: EntityT[Any]) -> EntityLabels:
        """Get labels from the configured Ozone instance"""
        entity_key = str(entity.id)
        entity_key = unquote(entity_key)

        # TODO: support other stuff
        if not entity_key.startswith('did:'):
            return create_empty_entity_labels()

        return self._client.get_did_labels(did=entity_key)

    @contextmanager
    def read_modify_write_labels_atomically(self, entity: EntityT[Any]) -> Generator[EntityLabels, None, None]:
        """Update an entity's labels in Ozone."""

        entity_key = str(entity.id)
        entity_key = unquote(entity_key)

        # TODO: support other stuff
        if not entity_key.startswith('did:'):
            labels = create_empty_entity_labels()
            yield labels
            return

        labels = self._client.get_did_labels(did=entity_key)

        # because labels gets updated in-place, we want to store a copy of the old labels
        # so that we only add/remove the needed labels in ozone
        old_labels = copy.deepcopy(labels)

        try:
            yield labels
        except Exception:
            raise

        for val in old_labels.labels:
            if val not in labels.labels:
                self._client.add_or_remove_label(action_id=0, entity_id=entity_key, label=val, neg=True)

        for val in labels.labels:
            if val in old_labels.labels:
                continue

            self._client.add_or_remove_label(
                action_id=0,
                entity_id=entity_key,
                label=val,
                neg=False,
            )


def create_empty_entity_labels():
    labels: Dict[str, LabelState] = {}
    return EntityLabels(labels=labels)
