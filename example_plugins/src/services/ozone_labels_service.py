import logging
from typing import Any, Dict, Generator

import requests
from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_shared.labels import EntityLabels, LabelReasons, LabelState, LabelStatus
from osprey.worker.lib.storage.labels import LabelsServiceBase
from services.atproto import OzoneSession
from services.get_repo_model import OzoneGetRepoResponse

logger = logging.getLogger('ozone_labels_service')


class OzoneLabelsService(LabelsServiceBase):
    def __init__(self, config: Config):
        super().__init__()

        self._session = OzoneSession.get_instance(config=config)

    def initialize(self) -> None:
        # TODO: setup session
        pass

    def read_labels(self, entity: EntityT[Any]) -> EntityLabels:
        entity_key = str(entity.id)

        if not entity_key.startswith('did:'):
            return create_empty_entity_labels()

        return self._get_did_labels(entity_key)

    def read_modify_write_labels_atomically(self) -> Generator[EntityLabels, None, None]:
        raise NotImplementedError()

    def _get_did_labels(self, did: str) -> EntityLabels:
        params: Dict[str, Any] = {
            'did': did,
        }

        headers = self._session.get_headers_with_moderation()

        try:
            response = requests.get(
                f'{self._session.get_pds_url()}/xrpc/tools.ozone.moderation.getRepo',
                headers=headers,
                params=params,
            )
            response.raise_for_status()
        except Exception as e:
            logger.error(f'Failed to get repo for {did}: {e}')
            raise e

        try:
            repo = OzoneGetRepoResponse.parse_obj(response.json())
        except Exception as e:
            logger.error(f'Failed to parse response JSON: {e}')
            raise e

        if not repo.labels:
            return create_empty_entity_labels()

        labels: Dict[str, LabelState] = {}

        if repo.labels:
            for label in repo.labels:
                if not label.neg:
                    continue
                labels[label.val] = LabelState(status=LabelStatus.ADDED, reasons=LabelReasons())

        return EntityLabels(labels=labels)


def create_empty_entity_labels():
    labels: Dict[str, LabelState] = {}
    return EntityLabels(labels=labels)
