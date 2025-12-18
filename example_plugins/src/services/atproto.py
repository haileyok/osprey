import logging
import threading
from datetime import datetime, timedelta
from typing import Optional

import requests
from osprey.worker.lib.config import Config

logger = logging.getLogger('ozone_session')


class OzoneSession:
    _instance: Optional['OzoneSession'] = None
    _init_lock = threading.Lock()

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
        self._labeler_did = config.get_optional_str('OSPREY_BLUESKY_LABELER_DID')
        if not self._identifier or not self._password:
            raise ValueError(
                'OSPREY_BLUESKY_IDENTIFIER and OSPREY_BLUESKY_PASSWORD must be set in environment variables'
            )
        self._create_session()

    @classmethod
    def get_instance(cls, config: Config) -> 'OzoneSession':
        if cls._instance is None:
            with cls._init_lock:
                if cls._instance is None:
                    cls._instance = cls(config)
        return cls._instance

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

    def get_pds_url(self):
        return self._pds_url

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

    def get_headers_with_moderation(self):
        """Get the headers, refreshing if necessary. Includes the proxy headers required for talking to Ozone"""
        with self._session_lock:
            if self._last_refresh is None or datetime.now() - self._last_refresh >= self._refresh_interval:
                self._refresh_session()

            labeler_did = self._labeler_did
            if labeler_did is None:
                labeler_did = self._did

            return {
                'accept': '*/*',
                'content-type': 'application/json',
                'authorization': f'Bearer {self._access_jwt}',
                'atproto-accept-labelers': f'{labeler_did};redact',
                'atproto-proxy': f'{labeler_did}#atproto_labeler',
            }
