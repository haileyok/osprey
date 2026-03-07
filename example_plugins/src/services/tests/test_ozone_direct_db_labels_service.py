from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from osprey.engine.language_types.entities import EntityT
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_shared.labels import EntityLabels, LabelReasons, LabelStatus
from osprey.worker.lib.storage.postgres import scoped_session
from services.ozone_direct_db_labels_service import OzoneDirectDbLabelsService
from services.ozone_label_model import OzoneLabelModel, ozone_metadata
from sqlalchemy import insert


@pytest.fixture
def label_table_cleanup() -> Iterator[None]:
    """Function-scoped fixture that cleans the label table before and after each test."""
    # Clean up the label table before the test
    with scoped_session(database='ozone_db') as session:
        session.query(OzoneLabelModel).delete()
        session.commit()

    yield

    # Clean up the label table after each test
    with scoped_session(database='ozone_db') as session:
        session.query(OzoneLabelModel).delete()
        session.commit()


@pytest.fixture(scope='session', autouse=True)
def ensure_label_table_exists() -> Iterator[None]:
    """Session-scoped fixture that ensures the label table exists."""
    with scoped_session(database='ozone_db') as session:
        engine = session.get_bind()
        ozone_metadata.create_all(engine)
    return


@pytest.fixture
def mock_config() -> Config:
    """Fixture that provides a mock Config."""
    config = MagicMock(spec=Config)
    return config


@pytest.fixture
def mock_entity_did() -> EntityT[Any]:
    """Fixture that provides a mock entity with a DID."""
    entity = MagicMock(spec=EntityT)
    entity.id = 'did:plc:example123'
    return entity


@pytest.fixture
def mock_entity_non_did() -> EntityT[Any]:
    """Fixture that provides a mock entity with a non-DID URI."""
    entity = MagicMock(spec=EntityT)
    entity.id = 'at://did:plc:example123/app.bsky.feed.post/456'
    return entity


def seed_label(
    session: Any,
    label_id: int,
    uri: str,
    val: str,
    neg: bool | None,
    exp: str | None,
    src: str = 'atproto:moderation/c',
    cid: str = 'bafy1234',
    cts: str = '2026-02-21T00:00:00.000Z',
) -> None:
    """Helper to seed a label into the label table."""
    stmt = insert(OzoneLabelModel).values(id=label_id, src=src, uri=uri, cid=cid, val=val, neg=neg, cts=cts, exp=exp)
    session.execute(stmt)
    session.commit()


class TestReadLabelsActiveLabels:
    """Test AC2.1: read_labels returns active labels with ADDED status."""

    def test_read_labels_returns_active_labels(self, label_table_cleanup, mock_config, mock_entity_did):
        """Test that read_labels returns active labels with LabelStatus.ADDED."""
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            service = OzoneDirectDbLabelsService(mock_config)

            # Seed active labels
            with scoped_session(database='ozone_db') as session:
                seed_label(session, 1, 'did:plc:example123', 'policy-spam', neg=False, exp=None)
                seed_label(session, 2, 'did:plc:example123', 'policy-adult', neg=False, exp=None)

            # Call read_labels
            labels = service.read_labels(mock_entity_did)

            # Verify results
            assert 'policy-spam' in labels.labels
            assert 'policy-adult' in labels.labels
            assert labels.labels['policy-spam'].status == LabelStatus.ADDED
            assert labels.labels['policy-adult'].status == LabelStatus.ADDED
            assert labels.labels['policy-spam'].reasons == LabelReasons()
            assert labels.labels['policy-adult'].reasons == LabelReasons()

    def test_read_labels_multiple_entities(self, label_table_cleanup, mock_config):
        """Test that read_labels returns only labels for the queried entity."""
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            service = OzoneDirectDbLabelsService(mock_config)

            # Seed labels for two different entities
            with scoped_session(database='ozone_db') as session:
                seed_label(session, 1, 'did:plc:example123', 'spam', neg=False, exp=None)
                seed_label(session, 2, 'did:plc:other456', 'adult', neg=False, exp=None)

            entity1 = MagicMock(spec=EntityT)
            entity1.id = 'did:plc:example123'

            labels = service.read_labels(entity1)

            assert 'spam' in labels.labels
            assert 'adult' not in labels.labels
            assert len(labels.labels) == 1


class TestReadLabelsNegation:
    """Test AC2.2: Negated labels are excluded from results."""

    def test_negated_label_excluded(self, label_table_cleanup, mock_config, mock_entity_did):
        """Test that labels with neg=True are excluded."""
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            service = OzoneDirectDbLabelsService(mock_config)

            with scoped_session(database='ozone_db') as session:
                # Add then negate a label
                seed_label(session, 1, 'did:plc:example123', 'spam', neg=False, exp=None)
                seed_label(session, 2, 'did:plc:example123', 'spam', neg=True, exp=None)

            labels = service.read_labels(mock_entity_did)

            assert 'spam' not in labels.labels

    def test_reapplied_label_included(self, label_table_cleanup, mock_config, mock_entity_did):
        """Test that labels reapplied after negation are included."""
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            service = OzoneDirectDbLabelsService(mock_config)

            with scoped_session(database='ozone_db') as session:
                # Add, negate, then reapply a label
                seed_label(session, 1, 'did:plc:example123', 'spam', neg=False, exp=None)
                seed_label(session, 2, 'did:plc:example123', 'spam', neg=True, exp=None)
                seed_label(session, 3, 'did:plc:example123', 'spam', neg=False, exp=None)

            labels = service.read_labels(mock_entity_did)

            assert 'spam' in labels.labels
            assert labels.labels['spam'].status == LabelStatus.ADDED

    def test_only_negation_event_excluded(self, label_table_cleanup, mock_config, mock_entity_did):
        """Test that a single negation event with no prior apply event excludes the label."""
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            service = OzoneDirectDbLabelsService(mock_config)

            with scoped_session(database='ozone_db') as session:
                # Only seed a negation event, no prior apply event
                seed_label(session, 1, 'did:plc:example123', 'spam', neg=True, exp=None)

            labels = service.read_labels(mock_entity_did)

            assert 'spam' not in labels.labels


class TestReadLabelsExpiration:
    """Test AC2.3: Expired labels are excluded from results."""

    def test_expired_label_excluded(self, label_table_cleanup, mock_config, mock_entity_did):
        """Test that labels with past exp timestamps are excluded."""
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            service = OzoneDirectDbLabelsService(mock_config)

            # Create a past timestamp
            past_time = (datetime.now(UTC) - timedelta(hours=1)).isoformat()

            with scoped_session(database='ozone_db') as session:
                seed_label(session, 1, 'did:plc:example123', 'spam', neg=False, exp=past_time)

            labels = service.read_labels(mock_entity_did)

            assert 'spam' not in labels.labels

    def test_future_expiry_label_included(self, label_table_cleanup, mock_config, mock_entity_did):
        """Test that labels with future exp timestamps are included."""
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            service = OzoneDirectDbLabelsService(mock_config)

            # Create a future timestamp
            future_time = (datetime.now(UTC) + timedelta(hours=1)).isoformat()

            with scoped_session(database='ozone_db') as session:
                seed_label(session, 1, 'did:plc:example123', 'spam', neg=False, exp=future_time)

            labels = service.read_labels(mock_entity_did)

            assert 'spam' in labels.labels
            assert labels.labels['spam'].status == LabelStatus.ADDED

    def test_unparseable_exp_skipped_with_warning(self, label_table_cleanup, mock_config, mock_entity_did):
        """Test that labels with unparseable exp are skipped with a warning."""
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            service = OzoneDirectDbLabelsService(mock_config)

            with scoped_session(database='ozone_db') as session:
                seed_label(session, 1, 'did:plc:example123', 'spam', neg=False, exp='not-a-date')

            with patch('services.ozone_direct_db_labels_service.logger') as mock_logger:
                labels = service.read_labels(mock_entity_did)

                assert 'spam' not in labels.labels
                mock_logger.warning.assert_called()


class TestReadLabelsNonDID:
    """Test AC2.4: Non-DID entities return empty EntityLabels."""

    def test_non_did_entity_returns_empty(self, label_table_cleanup, mock_config, mock_entity_non_did):
        """Test that non-DID entities return empty labels."""
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            service = OzoneDirectDbLabelsService(mock_config)

            labels = service.read_labels(mock_entity_non_did)

            assert len(labels.labels) == 0
            assert isinstance(labels, EntityLabels)

    def test_url_decoded_entity_id(self, label_table_cleanup, mock_config):
        """Test that URL-encoded entity IDs are properly decoded."""
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            service = OzoneDirectDbLabelsService(mock_config)

            entity = MagicMock(spec=EntityT)
            entity.id = 'did%3Aplc%3Aexample123'  # URL-encoded did:plc:example123

            with scoped_session(database='ozone_db') as session:
                seed_label(session, 1, 'did:plc:example123', 'spam', neg=False, exp=None)

            labels = service.read_labels(entity)

            assert 'spam' in labels.labels


class TestReadLabelsException:
    """Test AC2.5: Database exceptions propagate."""

    def test_database_exception_propagates(self, mock_config, mock_entity_did):
        """Test that database connection failures propagate exceptions."""
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            service = OzoneDirectDbLabelsService(mock_config, database='nonexistent_db')

            with pytest.raises(Exception):
                service.read_labels(mock_entity_did)


class TestReadModifyWriteLabelsAtomically:
    """Test read_modify_write_labels_atomically with label deltas."""

    def test_read_modify_write_adds_label(self, label_table_cleanup, mock_config, mock_entity_did):
        """Test that added labels are written via OzoneClient."""
        mock_client = MagicMock()
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance', return_value=mock_client):
            service = OzoneDirectDbLabelsService(mock_config)

            # Seed an existing label
            with scoped_session(database='ozone_db') as session:
                seed_label(session, 1, 'did:plc:example123', 'spam', neg=False, exp=None)

            # Use the context manager to add a label
            with service.read_modify_write_labels_atomically(mock_entity_did) as labels:
                from osprey.worker.lib.osprey_shared.labels import LabelReasons, LabelState, LabelStatus

                labels.labels['new-label'] = LabelState(status=LabelStatus.ADDED, reasons=LabelReasons())

            # Verify OzoneClient was called to add the new label
            mock_client.add_or_remove_label.assert_called()
            calls = mock_client.add_or_remove_label.call_args_list
            assert any(
                call[1].get('label') == 'new-label'
                and call[1].get('neg') is False
                and call[1].get('entity_id') == 'did:plc:example123'
                for call in calls
            )

    def test_read_modify_write_removes_label(self, label_table_cleanup, mock_config, mock_entity_did):
        """Test that removed labels are written via OzoneClient."""
        mock_client = MagicMock()
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance', return_value=mock_client):
            service = OzoneDirectDbLabelsService(mock_config)

            # Seed an existing label
            with scoped_session(database='ozone_db') as session:
                seed_label(session, 1, 'did:plc:example123', 'spam', neg=False, exp=None)

            # Use the context manager to remove a label
            with service.read_modify_write_labels_atomically(mock_entity_did) as labels:
                labels.labels.pop('spam', None)

            # Verify OzoneClient was called to remove the label
            mock_client.add_or_remove_label.assert_called()
            calls = mock_client.add_or_remove_label.call_args_list
            assert any(
                call[1].get('label') == 'spam'
                and call[1].get('neg') is True
                and call[1].get('entity_id') == 'did:plc:example123'
                for call in calls
            )

    def test_read_modify_write_non_did_returns_empty(self, label_table_cleanup, mock_config, mock_entity_non_did):
        """Test that non-DID entities yield empty labels."""
        mock_client = MagicMock()
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance', return_value=mock_client):
            service = OzoneDirectDbLabelsService(mock_config)

            with service.read_modify_write_labels_atomically(mock_entity_non_did) as labels:
                assert len(labels.labels) == 0

            # Verify OzoneClient was not called
            mock_client.add_or_remove_label.assert_not_called()


class TestMostRecentEventResolution:
    """Test that most-recent events are correctly resolved."""

    def test_most_recent_event_per_label_value(self, label_table_cleanup, mock_config, mock_entity_did):
        """Test that only the most recent event per label value is considered."""
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            service = OzoneDirectDbLabelsService(mock_config)

            with scoped_session(database='ozone_db') as session:
                # Multiple events for same label value
                seed_label(session, 1, 'did:plc:example123', 'spam', neg=False, exp=None)
                seed_label(session, 2, 'did:plc:example123', 'spam', neg=True, exp=None)
                seed_label(session, 3, 'did:plc:example123', 'spam', neg=False, exp=None)

            labels = service.read_labels(mock_entity_did)

            # Most recent event (id=3) has neg=False, so label should be included
            assert 'spam' in labels.labels

    def test_multiple_labels_independent_events(self, label_table_cleanup, mock_config, mock_entity_did):
        """Test that different labels have independent event histories."""
        with patch('services.ozone_direct_db_labels_service.OzoneClient.get_instance'):
            service = OzoneDirectDbLabelsService(mock_config)

            with scoped_session(database='ozone_db') as session:
                # Different labels with different histories
                seed_label(session, 1, 'did:plc:example123', 'spam', neg=False, exp=None)
                seed_label(session, 2, 'did:plc:example123', 'adult', neg=False, exp=None)
                seed_label(session, 3, 'did:plc:example123', 'spam', neg=True, exp=None)

            labels = service.read_labels(mock_entity_did)

            # Spam was negated (most recent event), adult was not
            assert 'spam' not in labels.labels
            assert 'adult' in labels.labels
