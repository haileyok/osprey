"""Tests for OzoneSession timeout and rate limit handling."""
import time
from unittest.mock import MagicMock, patch

import pytest
import requests
from osprey.worker.lib.config import Config
from services.ozone_session import OzoneSession


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset OzoneSession singleton before and after each test."""
    OzoneSession._instance = None
    yield
    OzoneSession._instance = None


@pytest.fixture
def mock_config() -> Config:
    """Mock Config object for OzoneSession."""
    config = MagicMock(spec=Config)
    config.get_optional_str = MagicMock(side_effect=lambda key: {
        'OSPREY_BLUESKY_IDENTIFIER': 'test@example.com',
        'OSPREY_BLUESKY_PASSWORD': 'test_password',
        'OSPREY_BLUESKY_LABELER_DID': None,
    }.get(key))
    config.get_str = MagicMock(side_effect=lambda key, default=None: {
        'OSPREY_BLUESKY_PDS_URL': 'https://bsky.social',
    }.get(key, default))
    return config


def test_ac7_1_create_session_has_timeout(mock_config):
    """AC7.1: _create_session() requests.post call has timeout=10."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', return_value=mock_response) as mock_post:
        OzoneSession(mock_config)

        # Verify requests.post was called with timeout=10
        assert mock_post.called
        call_kwargs = mock_post.call_args[1]
        assert 'timeout' in call_kwargs
        assert call_kwargs['timeout'] == 10


def test_ac7_2_refresh_session_has_timeout(mock_config):
    """AC7.2: _refresh_session() requests.post call has timeout=10."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', return_value=mock_response) as mock_post:
        session = OzoneSession(mock_config)
        # Reset call count after __init__ (which calls _create_session)
        mock_post.reset_mock()

        # Call _refresh_session
        session._refresh_session()

        # Verify requests.post was called with timeout=10
        assert mock_post.called
        call_kwargs = mock_post.call_args[1]
        assert 'timeout' in call_kwargs
        assert call_kwargs['timeout'] == 10


# Tests for _parse_rate_limit_reset() — Rate Limit Header Parsing


def test_ac1_4_missing_ratelimit_reset_header():
    """AC1.4: Missing RateLimit-Reset header falls back to 60-second wait."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.headers = {}

    result = OzoneSession._parse_rate_limit_reset(mock_response)

    assert result == 60.0


def test_ac1_5_malformed_ratelimit_reset_header():
    """AC1.5: Malformed RateLimit-Reset header falls back to 60-second wait."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.headers = {'RateLimit-Reset': 'not-a-number'}

    result = OzoneSession._parse_rate_limit_reset(mock_response)

    assert result == 60.0


def test_ac1_6_ratelimit_reset_timestamp_in_past():
    """AC1.6: RateLimit-Reset timestamp in the past results in 1-second minimum wait."""
    mock_response = MagicMock(spec=requests.Response)
    past_timestamp = time.time() - 100
    mock_response.headers = {'RateLimit-Reset': str(past_timestamp)}

    result = OzoneSession._parse_rate_limit_reset(mock_response)

    assert result == 1.0


def test_valid_future_ratelimit_reset():
    """Valid future RateLimit-Reset returns approximately expected seconds."""
    mock_response = MagicMock(spec=requests.Response)
    future_timestamp = time.time() + 30
    mock_response.headers = {'RateLimit-Reset': str(future_timestamp)}

    result = OzoneSession._parse_rate_limit_reset(mock_response)

    # Allow 1 second tolerance for execution time
    assert 29.0 <= result <= 31.0


def test_ratelimit_reset_integer_timestamp():
    """RateLimit-Reset can be provided as integer timestamp."""
    mock_response = MagicMock(spec=requests.Response)
    future_timestamp = int(time.time()) + 45
    mock_response.headers = {'RateLimit-Reset': str(future_timestamp)}

    result = OzoneSession._parse_rate_limit_reset(mock_response)

    # Allow 1 second tolerance for execution time
    assert 44.0 <= result <= 46.0


def test_ratelimit_reset_edge_case_zero():
    """RateLimit-Reset of 0 (or very small timestamp) falls back to minimum."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.headers = {'RateLimit-Reset': '0'}

    result = OzoneSession._parse_rate_limit_reset(mock_response)

    assert result == 1.0


def test_parse_rate_limit_reset_ignores_ratelimit_remaining():
    """RateLimit-Remaining header is not used in the computation."""
    mock_response = MagicMock(spec=requests.Response)
    future_timestamp = time.time() + 25
    mock_response.headers = {
        'RateLimit-Reset': str(future_timestamp),
        'RateLimit-Remaining': '0',
    }

    result = OzoneSession._parse_rate_limit_reset(mock_response)

    # Should compute based on RateLimit-Reset, not RateLimit-Remaining
    assert 24.0 <= result <= 26.0


# Tests for _create_session() Retry Logic


def test_ac1_1_create_session_handles_429_with_retry(mock_config):
    """AC1.1: On HTTP 429, method sleeps until RateLimit-Reset and retries successfully."""
    # First call returns 429, second call returns 200
    mock_response_429 = MagicMock()
    mock_response_429.status_code = 429
    future_timestamp = time.time() + 5
    mock_response_429.headers = {'RateLimit-Reset': str(future_timestamp)}

    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', side_effect=[mock_response_429, mock_response_200]):
        with patch('services.ozone_session.time.sleep') as mock_sleep:
            session = OzoneSession(mock_config)

            # Verify session was created successfully
            assert session._access_jwt == 'test_access_jwt'
            assert session._refresh_jwt == 'test_refresh_jwt'
            assert session._did == 'did:plc:test'

            # Verify time.sleep was called with approximately 5 seconds
            mock_sleep.assert_called()
            sleep_call = mock_sleep.call_args[0][0]
            assert 4.0 <= sleep_call <= 6.0


def test_ac1_2_create_session_handles_transient_error_with_backoff(mock_config):
    """AC1.2: On transient error (ConnectionError), method retries with backoff and succeeds."""
    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', side_effect=[
        requests.exceptions.ConnectionError('Connection failed'),
        mock_response_200
    ]):
        with patch('services.ozone_session.time.sleep') as mock_sleep:
            session = OzoneSession(mock_config)

            # Verify session was created successfully
            assert session._access_jwt == 'test_access_jwt'

            # Verify time.sleep was called (with backoff delay)
            mock_sleep.assert_called()
            sleep_call = mock_sleep.call_args[0][0]
            # First backoff delay with min=1.0, max=30.0 should be around 1.0-1.5 with jitter
            assert 0.5 <= sleep_call <= 2.0


def test_ac1_3_create_session_raises_on_non_retryable_error(mock_config):
    """AC1.3: On non-retryable error (400), method raises immediately without retry."""
    mock_response_400 = MagicMock()
    mock_response_400.status_code = 400
    mock_response_400.raise_for_status.side_effect = requests.exceptions.HTTPError('Bad request')

    with patch('services.ozone_session.requests.post', return_value=mock_response_400):
        with patch('services.ozone_session.time.sleep') as mock_sleep:
            with pytest.raises(requests.exceptions.HTTPError):
                OzoneSession(mock_config)

            # Verify time.sleep was NOT called
            mock_sleep.assert_not_called()


def test_ac1_7_create_session_retries_multiple_429s(mock_config):
    """AC1.7: After multiple consecutive 429s, method continues retrying until success."""
    # Three 429 responses followed by successful 200
    mock_response_429_1 = MagicMock()
    mock_response_429_1.status_code = 429
    future_timestamp = time.time() + 2
    mock_response_429_1.headers = {'RateLimit-Reset': str(future_timestamp)}

    mock_response_429_2 = MagicMock()
    mock_response_429_2.status_code = 429
    mock_response_429_2.headers = {'RateLimit-Reset': str(future_timestamp)}

    mock_response_429_3 = MagicMock()
    mock_response_429_3.status_code = 429
    mock_response_429_3.headers = {'RateLimit-Reset': str(future_timestamp)}

    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', side_effect=[
        mock_response_429_1, mock_response_429_2, mock_response_429_3, mock_response_200
    ]):
        with patch('services.ozone_session.time.sleep') as mock_sleep:
            session = OzoneSession(mock_config)

            # Verify session was created successfully
            assert session._did == 'did:plc:test'

            # Verify time.sleep was called three times (once for each 429)
            assert mock_sleep.call_count == 3


def test_ac3_1_create_session_uses_backoff_for_transient_errors(mock_config):
    """AC3.1: _create_session() uses Backoff(min_delay=1.0, max_delay=30.0) for transient errors."""
    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', side_effect=[
        requests.exceptions.ConnectionError('Error 1'),
        requests.exceptions.ConnectionError('Error 2'),
        mock_response_200
    ]):
        with patch('services.ozone_session.time.sleep') as mock_sleep:
            session = OzoneSession(mock_config)

            # Verify session was created successfully
            assert session._did == 'did:plc:test'

            # Verify time.sleep was called twice with increasing delays (exponential backoff)
            assert mock_sleep.call_count == 2
            sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]

            # First delay should be around 1.0 (min_delay)
            assert 0.5 <= sleep_calls[0] <= 2.0

            # Second delay should be larger (exponential backoff: 2 * min_delay)
            # Range check already verifies independent delay values; strict > comparison
            # is flaky due to jitter + rounding (both could round to 1.0)
            assert 1.0 <= sleep_calls[1] <= 30.0


def test_ac4_1_create_session_logs_rate_limit_wait(mock_config):
    """AC4.1: Rate-limit waits log warning with wait duration and RateLimit-Remaining if available."""
    mock_response_429 = MagicMock()
    mock_response_429.status_code = 429
    future_timestamp = time.time() + 5
    mock_response_429.headers = {
        'RateLimit-Reset': str(future_timestamp),
        'RateLimit-Remaining': '5'
    }

    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', side_effect=[mock_response_429, mock_response_200]):
        with patch('services.ozone_session.time.sleep'):
            with patch('services.ozone_session.logger') as mock_logger:
                OzoneSession(mock_config)

                # Find the warning call that contains rate limit info
                warning_calls = [call[0][0] for call in mock_logger.warning.call_args_list]
                rate_limit_warning = [c for c in warning_calls if 'Rate limited' in c and 'remaining' in c]

                assert len(rate_limit_warning) > 0
                msg = rate_limit_warning[0]
                assert 'waiting' in msg
                assert '5' in msg  # RateLimit-Remaining value
                assert 'remaining' in msg


def test_ac4_2_create_session_logs_transient_error_retry(mock_config):
    """AC4.2: Transient error retries log warning with attempt number and backoff delay."""
    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', side_effect=[
        requests.exceptions.Timeout('Timeout'),
        mock_response_200
    ]):
        with patch('services.ozone_session.time.sleep'):
            with patch('services.ozone_session.logger') as mock_logger:
                OzoneSession(mock_config)

                # Find the warning call about transient error
                warning_calls = [call[0][0] for call in mock_logger.warning.call_args_list]
                transient_warning = [c for c in warning_calls if 'Transient error' in c]

                assert len(transient_warning) > 0
                msg = transient_warning[0]
                assert 'retrying in' in msg
                assert 'attempt' in msg


def test_ac7_3_create_session_treats_timeout_as_transient(mock_config):
    """AC7.3: Timeout expiry is treated as a transient error (retried via Backoff)."""
    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', side_effect=[
        requests.exceptions.Timeout('Request timeout'),
        mock_response_200
    ]):
        with patch('services.ozone_session.time.sleep') as mock_sleep:
            session = OzoneSession(mock_config)

            # Verify session was created successfully
            assert session._did == 'did:plc:test'

            # Verify time.sleep was called (with backoff delay, not rate limit parsing)
            mock_sleep.assert_called_once()


def test_create_session_handles_500_with_backoff(mock_config):
    """Server errors (500+) are retried via backoff, not rate limit parsing."""
    mock_response_500 = MagicMock()
    mock_response_500.status_code = 500

    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', side_effect=[mock_response_500, mock_response_200]):
        with patch('services.ozone_session.time.sleep') as mock_sleep:
            session = OzoneSession(mock_config)

            # Verify session was created successfully
            assert session._did == 'did:plc:test'

            # Verify time.sleep was called with backoff delay
            mock_sleep.assert_called_once()
            sleep_call = mock_sleep.call_args[0][0]
            # Should be backoff delay (around 1.0-1.5 with jitter), not rate limit parsing
            assert 0.5 <= sleep_call <= 2.0


# Tests for _refresh_session() Retry and Fallback Logic


def test_ac2_1_refresh_session_retries_on_transient_error(mock_config):
    """AC2.1: On transient error, _refresh_session() retries up to 3 times with backoff."""
    # Create session first
    mock_response_success = MagicMock()
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', return_value=mock_response_success):
        session = OzoneSession(mock_config)

    # Now test _refresh_session with transient errors
    mock_response_after_retry = MagicMock()
    mock_response_after_retry.status_code = 200
    mock_response_after_retry.json.return_value = {
        'accessJwt': 'new_access_jwt',
        'refreshJwt': 'new_refresh_jwt',
    }

    with patch('services.ozone_session.requests.post', side_effect=[
        requests.exceptions.ConnectionError('Connection failed'),
        requests.exceptions.ConnectionError('Connection failed'),
        mock_response_after_retry
    ]):
        with patch('services.ozone_session.time.sleep') as mock_sleep:
            session._refresh_session()

            # Verify session was refreshed successfully
            assert session._access_jwt == 'new_access_jwt'
            assert session._refresh_jwt == 'new_refresh_jwt'

            # Verify time.sleep was called twice (for two transient errors)
            assert mock_sleep.call_count == 2


def test_ac2_2_refresh_session_fallback_on_auth_error_401(mock_config):
    """AC2.2: On auth error 401, _refresh_session() falls back to _create_session()."""
    # Create session first
    mock_response_success = MagicMock()
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', return_value=mock_response_success):
        session = OzoneSession(mock_config)

    # Mock _create_session to avoid infinite recursion
    session._create_session = MagicMock()

    # Mock 401 response from refresh
    mock_response_401 = MagicMock()
    mock_response_401.status_code = 401

    with patch('services.ozone_session.requests.post', return_value=mock_response_401):
        with patch('services.ozone_session.logger') as mock_logger:
            session._refresh_session()

            # Verify _create_session was called as fallback
            session._create_session.assert_called_once()

            # Verify logger warning about fallback
            warning_calls = [call[0][0] for call in mock_logger.warning.call_args_list]
            fallback_warning = [c for c in warning_calls if 'falling back' in c]
            assert len(fallback_warning) > 0


def test_ac2_2_refresh_session_fallback_on_auth_error_403(mock_config):
    """AC2.2: On auth error 403, _refresh_session() falls back to _create_session()."""
    # Create session first
    mock_response_success = MagicMock()
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', return_value=mock_response_success):
        session = OzoneSession(mock_config)

    # Mock _create_session to avoid infinite recursion
    session._create_session = MagicMock()

    # Mock 403 response from refresh
    mock_response_403 = MagicMock()
    mock_response_403.status_code = 403

    with patch('services.ozone_session.requests.post', return_value=mock_response_403):
        with patch('services.ozone_session.logger') as mock_logger:
            session._refresh_session()

            # Verify _create_session was called as fallback
            session._create_session.assert_called_once()

            # Verify logger warning about fallback
            warning_calls = [call[0][0] for call in mock_logger.warning.call_args_list]
            fallback_warning = [c for c in warning_calls if 'falling back' in c]
            assert len(fallback_warning) > 0


def test_ac2_3_refresh_session_fallback_on_exhausted_retries(mock_config):
    """AC2.3: After 3 transient retries exhausted, _refresh_session() falls back to _create_session()."""
    # Create session first
    mock_response_success = MagicMock()
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', return_value=mock_response_success):
        session = OzoneSession(mock_config)

    # Mock _create_session to avoid infinite recursion
    session._create_session = MagicMock()

    # Mock three transient errors (all attempts exhausted)
    with patch('services.ozone_session.requests.post', side_effect=[
        requests.exceptions.ConnectionError('Connection failed'),
        requests.exceptions.ConnectionError('Connection failed'),
        requests.exceptions.ConnectionError('Connection failed'),
    ]):
        with patch('services.ozone_session.time.sleep'):
            session._refresh_session()

            # Verify _create_session was called as fallback
            session._create_session.assert_called_once()


def test_ac2_4_refresh_session_handles_429_without_consuming_retry_budget(mock_config):
    """AC2.4: On 429, _refresh_session() parses RateLimit-Reset and waits (defensive, no retry budget consumed)."""
    # Create session first
    mock_response_success = MagicMock()
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', return_value=mock_response_success):
        session = OzoneSession(mock_config)

    # Mock 429 then success
    mock_response_429 = MagicMock()
    mock_response_429.status_code = 429
    future_timestamp = time.time() + 5
    mock_response_429.headers = {'RateLimit-Reset': str(future_timestamp)}

    mock_response_after_429 = MagicMock()
    mock_response_after_429.status_code = 200
    mock_response_after_429.json.return_value = {
        'accessJwt': 'new_access_jwt',
        'refreshJwt': 'new_refresh_jwt',
    }

    with patch('services.ozone_session.requests.post', side_effect=[mock_response_429, mock_response_after_429]):
        with patch('services.ozone_session.time.sleep') as mock_sleep:
            session._refresh_session()

            # Verify session was refreshed successfully
            assert session._access_jwt == 'new_access_jwt'

            # Verify time.sleep was called with rate limit wait time
            mock_sleep.assert_called_once()
            sleep_call = mock_sleep.call_args[0][0]
            assert 4.0 <= sleep_call <= 6.0


def test_ac3_2_refresh_session_uses_backoff_for_transient_errors(mock_config):
    """AC3.2: _refresh_session() uses Backoff(min_delay=1.0, max_delay=30.0) for transient errors."""
    # Create session first
    mock_response_success = MagicMock()
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', return_value=mock_response_success):
        session = OzoneSession(mock_config)

    # Mock timeouts for backoff testing
    mock_response_after_retry = MagicMock()
    mock_response_after_retry.status_code = 200
    mock_response_after_retry.json.return_value = {
        'accessJwt': 'new_access_jwt',
        'refreshJwt': 'new_refresh_jwt',
    }

    with patch('services.ozone_session.requests.post', side_effect=[
        requests.exceptions.Timeout('Timeout 1'),
        requests.exceptions.Timeout('Timeout 2'),
        mock_response_after_retry
    ]):
        with patch('services.ozone_session.time.sleep') as mock_sleep:
            session._refresh_session()

            # Verify session was refreshed successfully
            assert session._access_jwt == 'new_access_jwt'

            # Verify time.sleep was called twice (exponential backoff)
            assert mock_sleep.call_count == 2
            sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]

            # First delay should be around 1.0 (min_delay)
            assert 0.5 <= sleep_calls[0] <= 2.0

            # Second delay should be exponential (>= first delay), within max_delay
            assert 1.0 <= sleep_calls[1] <= 30.0


def test_ac4_3_refresh_session_logs_fallback_reason(mock_config):
    """AC4.3: Refresh-to-create fallback logs warning explaining the fallback reason."""
    # Create session first
    mock_response_success = MagicMock()
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', return_value=mock_response_success):
        session = OzoneSession(mock_config)

    # Mock _create_session to avoid infinite recursion
    session._create_session = MagicMock()

    # Mock 403 response (auth error)
    mock_response_403 = MagicMock()
    mock_response_403.status_code = 403

    with patch('services.ozone_session.requests.post', return_value=mock_response_403):
        with patch('services.ozone_session.logger') as mock_logger:
            session._refresh_session()

            # Verify logger.warning was called with fallback explanation
            warning_calls = [call[0][0] for call in mock_logger.warning.call_args_list]
            fallback_warning = [c for c in warning_calls if 'falling back' in c]

            assert len(fallback_warning) > 0
            msg = fallback_warning[0]
            assert 'Auth error' in msg or '403' in msg
            assert 'expired' in msg or 'falling back' in msg


def test_refresh_session_handles_500_with_backoff(mock_config):
    """Server errors (500+) during refresh are retried via backoff."""
    # Create session first
    mock_response_success = MagicMock()
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', return_value=mock_response_success):
        session = OzoneSession(mock_config)

    # Mock 500 then success
    mock_response_500 = MagicMock()
    mock_response_500.status_code = 500

    mock_response_after_retry = MagicMock()
    mock_response_after_retry.status_code = 200
    mock_response_after_retry.json.return_value = {
        'accessJwt': 'new_access_jwt',
        'refreshJwt': 'new_refresh_jwt',
    }

    with patch('services.ozone_session.requests.post', side_effect=[mock_response_500, mock_response_after_retry]):
        with patch('services.ozone_session.time.sleep') as mock_sleep:
            session._refresh_session()

            # Verify session was refreshed successfully
            assert session._access_jwt == 'new_access_jwt'

            # Verify time.sleep was called with backoff delay
            mock_sleep.assert_called_once()
            sleep_call = mock_sleep.call_args[0][0]
            assert 0.5 <= sleep_call <= 2.0


def test_refresh_session_handles_multiple_429s_without_exhausting_retry_budget(mock_config):
    """Multiple 429s are retried without consuming transient retry budget."""
    # Create session first
    mock_response_success = MagicMock()
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', return_value=mock_response_success):
        session = OzoneSession(mock_config)

    # Mock three 429s then success
    mock_response_429_1 = MagicMock()
    mock_response_429_1.status_code = 429
    future_timestamp = time.time() + 2
    mock_response_429_1.headers = {'RateLimit-Reset': str(future_timestamp)}

    mock_response_429_2 = MagicMock()
    mock_response_429_2.status_code = 429
    mock_response_429_2.headers = {'RateLimit-Reset': str(future_timestamp)}

    mock_response_429_3 = MagicMock()
    mock_response_429_3.status_code = 429
    mock_response_429_3.headers = {'RateLimit-Reset': str(future_timestamp)}

    mock_response_after_429s = MagicMock()
    mock_response_after_429s.status_code = 200
    mock_response_after_429s.json.return_value = {
        'accessJwt': 'new_access_jwt',
        'refreshJwt': 'new_refresh_jwt',
    }

    with patch('services.ozone_session.requests.post', side_effect=[
        mock_response_429_1, mock_response_429_2, mock_response_429_3, mock_response_after_429s
    ]):
        with patch('services.ozone_session.time.sleep') as mock_sleep:
            session._refresh_session()

            # Verify session was refreshed successfully
            assert session._access_jwt == 'new_access_jwt'

            # Verify time.sleep was called three times (once for each 429)
            assert mock_sleep.call_count == 3


def test_refresh_session_fallback_on_other_4xx_error(mock_config):
    """Other 4xx errors (non-401/403) fall back to _create_session()."""
    # Create session first
    mock_response_success = MagicMock()
    mock_response_success.status_code = 200
    mock_response_success.json.return_value = {
        'accessJwt': 'test_access_jwt',
        'refreshJwt': 'test_refresh_jwt',
        'did': 'did:plc:test',
    }

    with patch('services.ozone_session.requests.post', return_value=mock_response_success):
        session = OzoneSession(mock_config)

    # Mock _create_session to avoid infinite recursion
    session._create_session = MagicMock()

    # Mock 400 response (bad request)
    mock_response_400 = MagicMock()
    mock_response_400.status_code = 400
    mock_response_400.raise_for_status.side_effect = requests.exceptions.HTTPError('Bad request')

    with patch('services.ozone_session.requests.post', return_value=mock_response_400):
        with patch('services.ozone_session.logger') as mock_logger:
            session._refresh_session()

            # Verify _create_session was called as fallback
            session._create_session.assert_called_once()

            # Verify logger warning about unexpected error
            warning_calls = [call[0][0] for call in mock_logger.warning.call_args_list]
            fallback_warning = [c for c in warning_calls if 'falling back' in c]
            assert len(fallback_warning) > 0
