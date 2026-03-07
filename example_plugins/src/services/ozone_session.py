import threading
import time
from datetime import datetime, timedelta
from typing import Optional

import requests
from osprey.worker.lib.backoff import Backoff
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_shared.logging import get_logger

logger = get_logger('ozone_session')

_SESSION_TIMEOUT = 10
_RATE_LIMIT_DEFAULT_WAIT = 60.0
_RATE_LIMIT_MIN_WAIT = 1.0
_MAX_REFRESH_RETRIES = 3


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
        """Create a new session by authenticating against the PDS.

        Retries indefinitely on rate limits (429) and transient errors.
        Raises immediately on non-retryable errors (4xx except 429).
        """
        with self._session_lock:
            backoff = Backoff(min_delay=1.0, max_delay=30.0, jitter=True)

            while True:
                try:
                    response = requests.post(
                        f'{self._pds_url}/xrpc/com.atproto.server.createSession',
                        json={
                            'identifier': self._identifier,
                            'password': self._password,
                        },
                        timeout=_SESSION_TIMEOUT,
                    )

                    if response.status_code == 429:
                        wait_seconds = self._parse_rate_limit_reset(response)
                        remaining = response.headers.get('RateLimit-Remaining')
                        logger.warning(
                            f'Rate limited during session creation, '
                            f'waiting {wait_seconds:.1f}s until reset'
                            f'{f", remaining: {remaining}" if remaining is not None else ""}'
                        )
                        time.sleep(wait_seconds)
                        # Reset transient error backoff — rate limit waiting is a separate
                        # concern, and successful rate limit recovery means the server is healthy
                        backoff.succeed()
                        continue

                    if response.status_code >= 500:
                        delay = backoff.fail()
                        logger.warning(
                            f'Server error {response.status_code} during session creation, '
                            f'retrying in {delay:.1f}s (attempt {backoff.fails})'
                        )
                        time.sleep(delay)
                        continue

                    response.raise_for_status()

                    data = response.json()
                    self._access_jwt = data['accessJwt']
                    self._refresh_jwt = data['refreshJwt']
                    self._did = data['did']
                    self._last_refresh = datetime.now()

                    backoff.succeed()
                    logger.info(f'Bluesky session created for DID: {self._did}')
                    return

                except requests.exceptions.HTTPError:
                    # Non-retryable client error (4xx except 429, which is handled above)
                    logger.error(
                        f'Non-retryable error during session creation: '
                        f'{response.status_code}'
                    )
                    raise

                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                    delay = backoff.fail()
                    logger.warning(
                        f'Transient error during session creation: {e}, '
                        f'retrying in {delay:.1f}s (attempt {backoff.fails})'
                    )
                    time.sleep(delay)
                    continue

    def _refresh_session(self):
        """Refresh the current session using the refresh token.

        Retries up to 3 times on transient errors with exponential backoff.
        Rate limit (429) waits do not count against the retry budget.
        Falls back to _create_session() on auth errors (401/403) or after
        exhausting transient retries.
        """
        with self._session_lock:
            backoff = Backoff(min_delay=1.0, max_delay=30.0, jitter=True)
            transient_failures = 0

            while transient_failures < _MAX_REFRESH_RETRIES:
                try:
                    response = requests.post(
                        f'{self._pds_url}/xrpc/com.atproto.server.refreshSession',
                        headers={'authorization': f'Bearer {self._refresh_jwt}'},
                        timeout=_SESSION_TIMEOUT,
                    )

                    if response.status_code == 429:
                        # 429 waits don't consume transient retry budget — consistent
                        # with _create_session() where 429 retries are infinite
                        wait_seconds = self._parse_rate_limit_reset(response)
                        logger.warning(
                            f'Rate limited during session refresh, '
                            f'waiting {wait_seconds:.1f}s until reset'
                        )
                        time.sleep(wait_seconds)
                        backoff.succeed()
                        continue

                    if response.status_code in (401, 403):
                        logger.warning(
                            f'Auth error {response.status_code} during session refresh '
                            f'(refresh token likely expired), falling back to create_session'
                        )
                        self._create_session()
                        return

                    if response.status_code >= 500:
                        transient_failures += 1
                        delay = backoff.fail()
                        logger.warning(
                            f'Server error {response.status_code} during session refresh, '
                            f'retrying in {delay:.1f}s '
                            f'(transient failure {transient_failures}/{_MAX_REFRESH_RETRIES})'
                        )
                        time.sleep(delay)
                        continue

                    response.raise_for_status()

                    data = response.json()
                    self._access_jwt = data['accessJwt']
                    self._refresh_jwt = data['refreshJwt']
                    self._last_refresh = datetime.now()
                    return

                except requests.exceptions.HTTPError:
                    # Non-429, non-401/403 client error — fall back to create
                    logger.warning(
                        f'Unexpected HTTP error {response.status_code} during session refresh, '
                        f'falling back to create_session'
                    )
                    self._create_session()
                    return

                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                    transient_failures += 1
                    delay = backoff.fail()
                    logger.warning(
                        f'Transient error during session refresh: {e}, '
                        f'retrying in {delay:.1f}s '
                        f'(transient failure {transient_failures}/{_MAX_REFRESH_RETRIES})'
                    )
                    time.sleep(delay)
                    continue

            # All transient retries exhausted
            logger.warning(
                f'Session refresh failed after {_MAX_REFRESH_RETRIES} transient failures, '
                f'falling back to create_session'
            )
            self._create_session()

    @staticmethod
    def _parse_rate_limit_reset(response: requests.Response) -> float:
        """Parse the RateLimit-Reset header and return seconds to wait.

        Returns the number of seconds until the rate limit resets.
        Falls back to 60s if the header is missing or malformed.
        Floors at 1s if the reset timestamp is in the past.
        """
        reset_value = response.headers.get('RateLimit-Reset')
        if reset_value is None:
            logger.warning(
                'Rate limited but RateLimit-Reset header missing, '
                f'defaulting to {_RATE_LIMIT_DEFAULT_WAIT}s wait'
            )
            return _RATE_LIMIT_DEFAULT_WAIT

        try:
            reset_timestamp = float(reset_value)
        except (ValueError, TypeError):
            logger.warning(
                f'Rate limited but RateLimit-Reset header malformed: {reset_value!r}, '
                f'defaulting to {_RATE_LIMIT_DEFAULT_WAIT}s wait'
            )
            return _RATE_LIMIT_DEFAULT_WAIT

        wait_seconds = reset_timestamp - time.time()
        if wait_seconds < _RATE_LIMIT_MIN_WAIT:
            return _RATE_LIMIT_MIN_WAIT

        return wait_seconds

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
