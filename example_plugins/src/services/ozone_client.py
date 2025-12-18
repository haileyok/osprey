import threading
from typing import Any, Dict, Optional

import requests
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_shared.labels import EntityLabels, LabelReasons, LabelState, LabelStatus
from osprey.worker.lib.osprey_shared.logging import get_logger
from services.get_repo_model import OzoneGetRepoResponse
from services.ozone_session import OzoneSession

logger = get_logger('ozone_session')


class OzoneClient:
    _instance: Optional['OzoneClient'] = None
    _init_lock = threading.Lock()

    def __init__(self, config: Config):
        try:
            self._session = OzoneSession.get_instance(config=config)
        except Exception as e:
            self._session = None
            logger.error(f'Failed to create Bluesky session: {e}')

        self._pds_url = config.get_str('OSPREY_BLUESKY_PDS_URL', 'https://bsky.social')

    @classmethod
    def get_instance(cls, config: Config) -> 'OzoneClient':
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    cls._instance = cls(config)
        return cls._instance

    def get_session(self):
        return self._session

    def add_or_remove_label(
        self,
        action_id: int,
        entity_id: str,
        label: str,
        neg: bool,
        comment: Optional[str] = None,
        expiration_in_hours: Optional[int] = None,
        cid: Optional[str] = None,
    ):
        if self._session is None:
            logger.error('Bluesky session not initialized, cannot apply label')
            return

        try:
            subject: Dict[str, str] = {}
            if entity_id.startswith('did:'):
                subject['$type'] = 'com.atproto.admin.defs#repoRef'
                subject['did'] = entity_id
            elif entity_id.startswith('at://'):
                subject['$type'] = 'com.atproto.repo.strongRef'
                subject['cid'] = cid or ''
                subject['uri'] = entity_id

            payload: Dict[str, Any] = {
                'subject': subject,
                'createdBy': self._session.get_did(),
                'subjectBlobCids': [],
                'event': {
                    '$type': 'tools.ozone.moderation.defs#modEventLabel',
                    'comment': comment or '',
                    'createLabelVals': [label] if not neg else [],
                    'negateLabelVals': [label] if neg else [],
                    'durationInHours': expiration_in_hours or 0,
                },
                'modTool': {'name': 'osprey', 'meta': {'actionId': str(action_id)}},
            }

            headers = self._session.get_headers_with_moderation()

            response = requests.post(
                f'{self._pds_url}/xrpc/tools.ozone.moderation.emitEvent',
                headers=headers,
                json=payload,
            )
            response.raise_for_status()
        except Exception as e:
            logger.error(f'Failed to emit label event: {e}')

    def get_did_labels(self, did: str) -> EntityLabels:
        """Fetches labels from the configured ozone instance for a particular DID. Uses tools.ozone.moderation.getRepo."""

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
                if hasattr(label, 'neg') and label.neg:
                    labels[label.val] = LabelState(status=LabelStatus.REMOVED, reasons=LabelReasons())
                else:
                    labels[label.val] = LabelState(status=LabelStatus.ADDED, reasons=LabelReasons())

        return EntityLabels(labels=labels)


def create_empty_entity_labels():
    labels: Dict[str, LabelState] = {}
    return EntityLabels(labels=labels)
