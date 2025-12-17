import logging
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import requests
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.config import Config
from osprey.worker.sinks.sink.output_sink import BaseOutputSink
from udfs.atproto.label import AtprotoLabelEffect

logger = logging.getLogger('ozone_label_sink')


class BlueskySession:
    def __init__(self, config: Config):
        self._session_lock = threading.RLock()
        self._access_jwt: Optional[str] = None
        self._refresh_jwt: Optional[str] = None
        self._did: Optional[str] = None
        self._last_refresh = None
        self._refresh_interval = timedelta(minutes=30)

        self._identifier = config.get_optional_str('OSPREY_BLUESKY_IDENTIFIER')
        self._password = config.get_optional_str('OSPREY_BLUESKY_PASSWORD')
        self._pds_url = config.get_str('OSPREY_BLUESKY_PDS_URL', 'https://bsky.social')

        if not self._identifier or not self._password:
            raise ValueError(
                'OSPREY_BLUESKY_IDENTIFIER and OSPREY_BLUESKY_PASSWORD must be set in environment variables'
            )

        self._create_session()

    def _create_session(self):
        """Refresh the session by logging in or refreshing the token."""
        with self._session_lock:
            try:
                response = requests.post(
                    f'{self._pds_url}/xrpc/com.atproto.server.createSession',
                    json={
                        'identifier': self._identifier,
                        'password': self._password,
                    },
                )
                response.raise_for_status()
                data = response.json()

                self._access_jwt = data['accessJwt']
                self._refresh_jwt = data['refreshJwt']
                self._did = data['did']
                self._last_refresh = datetime.now()

                logger.info(f'Bluesky session refreshed for DID: {self._did}')
            except Exception as e:
                logger.error(f'Failed to refresh Bluesky session: {e}')
                raise

    def _refresh_session(self):
        """Refresh the current session"""
        with self._session_lock:
            response = requests.post(
                f'{self._pds_url}/xrpc/com.atproto.server.refreshSession',
                headers={'authorization': f'Bearer {self._refresh_jwt}'},
            )
            response.raise_for_status()
            data = response.json()
            self._access_jwt = data['accessJwt']
            self._refresh_jwt = data['refreshJwt']
            self._last_refresh = datetime.now()

    def get_did(self):
        return self._did

    def get_headers(self):
        """Get the headers, refreshing if necessary"""
        with self._session_lock:
            if self._last_refresh is None or datetime.now() - self._last_refresh >= self._refresh_interval:
                self._refresh_session()

            return {
                'accept': '*/*',
                'content-type': 'application/json',
                'authorization': f'Bearer {self._access_jwt}',
            }


class OzoneLabelSink(BaseOutputSink):
    def __init__(self, config: Config):
        try:
            self._session = BlueskySession(config=config)
        except Exception as e:
            self._session = None
            logger.error(f'Failed to create Bluesky session: {e}')

        self._labeler_did = config.get_str('OSPREY_BLUESKY_LABELER_DID', 'did:plc:123')
        self._pds_url = config.get_str('OSPREY_BLUESKY_PDS_URL', 'https://bsky.social')

        logger.info('Initialized Ozone labels sink')

    def will_do_work(self, result: ExecutionResult) -> bool:
        return len(result.effects) > 0

    def push(self, result: ExecutionResult) -> None:
        action_id = result.action.action_id

        for effects in result.effects.values():
            for effect in effects:
                if not isinstance(effect, AtprotoLabelEffect):
                    continue

                self._apply_label(action_id, effect)

    def _apply_label(self, action_id: int, effect: AtprotoLabelEffect):
        if self._session is None:
            logger.error('Bluesky session not initialized, cannot apply label')
            return

        try:
            subject: Dict[str, str] = {}
            if effect.entity.startswith('did:'):
                subject['$type'] = 'com.atproto.admin.defs#repoRef'
                subject['did'] = effect.entity
            elif effect.entity.startswith('at://'):
                subject['$type'] = 'com.atproto.repo.strongRef'
                subject['cid'] = effect.cid or ''
                subject['uri'] = effect.entity

            payload: Dict[str, Any] = {
                'subject': subject,
                'createdBy': self._session.get_did(),
                'subjectBlobCids': [],
                'event': {
                    '$type': 'tools.ozone.moderation.defs#modEventLabel',
                    'comment': effect.comment,
                    'createLabelVals': [effect.label],
                    'negateLabelVals': [],
                    'durationInHours': effect.expiration_in_hours or 0,
                },
                'modTool': {'name': 'osprey', 'meta': {'actionId': str(action_id)}},
            }

            headers = self._apply_moderation_headers(self._session.get_headers())

            response = requests.post(
                f'{self._pds_url}/xrpc/tools.ozone.moderation.emitEvent',
                headers=headers,
                json=payload,
            )
            response.raise_for_status()

            logger.info(f'Successfully emitted label event for {effect.entity}: {effect.label}')

        except Exception as e:
            logger.error(f'Failed to emit label event: {e}')

    def _apply_moderation_headers(self, base_headers: Dict[str, Any]):
        headers = base_headers.copy()
        headers['atproto-accept-labelers'] = f'{self._labeler_did};redact'
        headers['atproto-proxy'] = f'{self._labeler_did}#atproto_labeler'
        return headers

    def stop(self) -> None:
        pass
