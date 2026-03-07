from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from osprey.worker.lib.config import Config
from osprey.worker.lib.storage.labels import LabelsProvider
from services.ozone_direct_db_labels_service import OzoneDirectDbLabelsProvider


class TestOzoneDirectDbLabelsProviderCacheTTL:
    """Test AC3.1: cache_ttl() returns value from config."""

    def test_cache_ttl_returns_default_300_seconds(self) -> None:
        """Test that cache_ttl() returns 300 seconds by default."""
        mock_config = MagicMock(spec=Config)
        mock_config.get_int.return_value = 300

        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            provider = OzoneDirectDbLabelsProvider(config=mock_config)

            result = provider.cache_ttl()

            assert result == timedelta(seconds=300)
            mock_config.get_int.assert_called_once_with('OSPREY_LABELS_CACHE_TTL_SECONDS', 300)

    @pytest.mark.parametrize('ttl_seconds', [0, 60, 600])
    def test_cache_ttl_returns_custom_value(self, ttl_seconds: int) -> None:
        """Test that cache_ttl() returns custom value from config."""
        mock_config = MagicMock(spec=Config)
        mock_config.get_int.return_value = ttl_seconds

        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            provider = OzoneDirectDbLabelsProvider(config=mock_config)

            result = provider.cache_ttl()

            assert result == timedelta(seconds=ttl_seconds)


class TestOzoneDirectDbLabelsProviderInstance:
    """Test AC3.1: OzoneDirectDbLabelsProvider is a LabelsProvider."""

    def test_provider_is_instance_of_labels_provider(self) -> None:
        """Test that OzoneDirectDbLabelsProvider is an instance of LabelsProvider."""
        mock_config = MagicMock(spec=Config)
        mock_config.get_int.return_value = 300

        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            provider = OzoneDirectDbLabelsProvider(config=mock_config)

            assert isinstance(provider, LabelsProvider)


class TestPluginRegistrationHook:
    """Test AC3.2: register_labels_service_or_provider returns OzoneDirectDbLabelsProvider."""

    def test_hook_returns_labels_provider(self) -> None:
        """Test that register_labels_service_or_provider returns a LabelsProvider."""
        from register_plugins import register_labels_service_or_provider

        mock_config = MagicMock(spec=Config)
        mock_config.get_int.return_value = 300

        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            result = register_labels_service_or_provider(config=mock_config)

            assert isinstance(result, LabelsProvider)
            assert isinstance(result, OzoneDirectDbLabelsProvider)


class TestRegisterUDFsDoesNotHaveAtprotoLabel:
    """Test AC3.3: register_udfs() does not register HasAtprotoLabel UDF."""

    def test_register_udfs_does_not_include_has_atproto_label(self) -> None:
        """Test that register_udfs() does not register HasAtprotoLabel UDF."""
        from register_plugins import register_udfs

        udfs = register_udfs()

        udf_names = [udf.__name__ for udf in udfs]

        assert 'HasAtprotoLabel' not in udf_names

    def test_register_udfs_returns_list_of_udfs(self) -> None:
        """Test that register_udfs() returns a sequence of UDF classes."""
        from register_plugins import register_udfs

        udfs = register_udfs()

        assert isinstance(udfs, (list, tuple))
        assert len(udfs) > 0
