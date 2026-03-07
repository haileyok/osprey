"""Tests for OzoneClient singleton and session initialization."""
from unittest.mock import MagicMock, patch

import pytest
from osprey.worker.lib.config import Config
from services.ozone_client import OzoneClient


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset OzoneClient singleton before and after each test."""
    OzoneClient._instance = None
    yield
    OzoneClient._instance = None


@pytest.fixture
def mock_config() -> Config:
    """Mock Config object for OzoneClient."""
    config = MagicMock(spec=Config)
    config.get_str = MagicMock(side_effect=lambda key, default=None: {
        'OSPREY_BLUESKY_PDS_URL': 'https://bsky.social',
    }.get(key, default))
    return config


def test_ac5_1_singleton_not_poisoned_on_session_failure(mock_config):
    """AC5.1: OzoneClient._instance remains None if OzoneSession.get_instance() raises."""
    with patch('services.ozone_client.OzoneSession') as mock_session_class:
        mock_session_class.get_instance.side_effect = RuntimeError('Session init failed')

        with pytest.raises(RuntimeError, match='Session init failed'):
            OzoneClient.get_instance(mock_config)

        assert OzoneClient._instance is None


def test_ac5_2_retry_after_failed_init(mock_config):
    """AC5.2: After failed init, calling get_instance() again retries session creation."""
    call_count = 0

    def mock_get_instance(config):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError('Session init failed')
        # Return a mock session on the second call
        mock_session = MagicMock()
        mock_session.get_did.return_value = 'did:plc:test'
        return mock_session

    with patch('services.ozone_client.OzoneSession') as mock_session_class:
        mock_session_class.get_instance.side_effect = mock_get_instance

        # First call should fail
        with pytest.raises(RuntimeError, match='Session init failed'):
            OzoneClient.get_instance(mock_config)

        # _instance is already None here (AC5.1 guarantees this)
        # Second call should succeed
        client = OzoneClient.get_instance(mock_config)
        assert client is not None
        assert client._session is not None


def test_ac5_3_singleton_caching_after_success(mock_config):
    """AC5.3: After successful init, subsequent get_instance() calls return the same cached instance."""
    mock_session = MagicMock()
    mock_session.get_did.return_value = 'did:plc:test'

    with patch('services.ozone_client.OzoneSession') as mock_session_class:
        mock_session_class.get_instance.return_value = mock_session

        client1 = OzoneClient.get_instance(mock_config)
        client2 = OzoneClient.get_instance(mock_config)

        assert client1 is client2
        # Verify get_instance was called exactly once (singleton caching)
        assert mock_session_class.get_instance.call_count == 1


def test_ac6_1_add_did_to_list_handles_missing_session(mock_config):
    """AC6.1: add_did_to_list() logs error and returns gracefully when _session is None."""
    # Manually create a client with None session (to simulate initialization failure)
    client = OzoneClient.__new__(OzoneClient)
    client._session = None
    client._pds_url = 'https://bsky.social'

    # Should not raise AssertionError, should return gracefully
    with patch('services.ozone_client.logger') as mock_logger:
        client.add_did_to_list('did:plc:test', 'at://list_uri')
        mock_logger.error.assert_called_once_with('Bluesky session not initialized, cannot add DID to list')
