from __future__ import absolute_import

from datetime import datetime, timedelta, timezone
from typing import Any, Iterator
from unittest.mock import MagicMock

import pytest
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_shared.labels import (
    EntityLabels,
    LabelStatus,
)
from osprey.worker.lib.storage.postgres import scoped_session
from services.local_db_labels_service import LabelsModel, LocalDbLabelsService
from sqlalchemy.orm.session import Session


@pytest.fixture(autouse=True)
def sqlalchemy_session() -> Iterator[Session]:
    """Session fixture for integration tests with Postgres."""
    with scoped_session(database='osprey_db') as session:
        # Clean up any test data before test
        session.query(LabelsModel).filter(
            LabelsModel.uri.ilike('did:plc:testuser%')
        ).delete()
        session.commit()
        yield session
        # Clean up after test
        session.query(LabelsModel).filter(
            LabelsModel.uri.ilike('did:plc:testuser%')
        ).delete()
        session.commit()


@pytest.fixture
def mock_config() -> Config:
    """Mock Config object for LocalDbLabelsService."""
    config = MagicMock(spec=Config)
    config.get_str = MagicMock(side_effect=lambda key, default=None: {
        'OSPREY_LABELER_DIDS': '["did:plc:e4elbtctnfqocyfcml6h2lf7"]',
        'OSPREY_BLUESKY_LABELER_DID': 'did:plc:e4elbtctnfqocyfcml6h2lf7',
    }.get(key, default))
    return config


@pytest.fixture
def labels_service(mock_config: Config) -> LocalDbLabelsService:
    """LocalDbLabelsService instance initialized for testing."""
    service = LocalDbLabelsService(config=mock_config)
    service.initialize()
    return service


class MockEntity:
    """Mock entity for testing."""

    def __init__(self, entity_id: str) -> None:
        self.id = entity_id


def test_ac4_1_active_labels_are_found(
    sqlalchemy_session: Session, labels_service: LocalDbLabelsService
) -> None:
    """
    AC4.1: HasLabel UDF returns true for labels present in the local labels table.

    Verify that read_labels returns EntityLabels with status=ADDED for active labels.
    """
    # Insert test labels
    now = datetime.now(timezone.utc)
    test_entity_uri = 'did:plc:testuser1'
    test_labeler_did = 'did:plc:e4elbtctnfqocyfcml6h2lf7'

    label1 = LabelsModel(
        src=test_labeler_did,
        uri=test_entity_uri,
        val='spam',
        cts=now,
        exp=None,
    )
    label2 = LabelsModel(
        src=test_labeler_did,
        uri=test_entity_uri,
        val='nsfw',
        cts=now,
        exp=None,
    )

    sqlalchemy_session.add(label1)
    sqlalchemy_session.add(label2)
    sqlalchemy_session.commit()

    # Call read_labels
    entity = MockEntity(test_entity_uri)
    result = labels_service.read_labels(entity)

    # Verify EntityLabels contains both labels with ADDED status
    assert isinstance(result, EntityLabels)
    assert len(result.labels) == 2
    assert 'spam' in result.labels
    assert 'nsfw' in result.labels
    assert result.labels['spam'].status == LabelStatus.ADDED
    assert result.labels['nsfw'].status == LabelStatus.ADDED


def test_ac4_2_expired_labels_are_excluded(
    sqlalchemy_session: Session, labels_service: LocalDbLabelsService
) -> None:
    """
    AC4.2: HasLabel UDF returns false for expired labels (exp < now).

    Verify that read_labels excludes labels where exp < now.
    """
    now = datetime.now(timezone.utc)
    past = now - timedelta(hours=1)
    test_entity_uri = 'did:plc:testuser2'
    test_labeler_did = 'did:plc:e4elbtctnfqocyfcml6h2lf7'

    # Insert an expired label
    expired_label = LabelsModel(
        src=test_labeler_did,
        uri=test_entity_uri,
        val='expired-label',
        cts=now,
        exp=past,  # Expired in the past
    )

    sqlalchemy_session.add(expired_label)
    sqlalchemy_session.commit()

    # Call read_labels
    entity = MockEntity(test_entity_uri)
    result = labels_service.read_labels(entity)

    # Verify EntityLabels is empty (expired label should be excluded)
    assert isinstance(result, EntityLabels)
    assert len(result.labels) == 0


def test_ac4_3_deleted_labels_not_found(
    sqlalchemy_session: Session, labels_service: LocalDbLabelsService
) -> None:
    """
    AC4.3: HasLabel UDF returns false for labels that have been negated (deleted from table).

    Verify that read_labels does not return labels that have been deleted from the table.
    """
    now = datetime.now(timezone.utc)
    test_entity_uri = 'did:plc:testuser3'
    test_labeler_did = 'did:plc:e4elbtctnfqocyfcml6h2lf7'

    # Insert a label
    label = LabelsModel(
        src=test_labeler_did,
        uri=test_entity_uri,
        val='spam',
        cts=now,
        exp=None,
    )

    sqlalchemy_session.add(label)
    sqlalchemy_session.commit()

    # Verify label is returned before deletion
    entity = MockEntity(test_entity_uri)
    result_before = labels_service.read_labels(entity)
    assert len(result_before.labels) == 1
    assert 'spam' in result_before.labels

    # Delete the label (simulating negation from Go consumer)
    sqlalchemy_session.query(LabelsModel).filter(
        LabelsModel.uri == test_entity_uri,
        LabelsModel.val == 'spam',
    ).delete()
    sqlalchemy_session.commit()

    # Call read_labels after deletion
    result_after = labels_service.read_labels(entity)

    # Verify EntityLabels is empty (deleted label should not be found)
    assert isinstance(result_after, EntityLabels)
    assert len(result_after.labels) == 0


def test_no_labels_returns_empty_entity_labels(
    sqlalchemy_session: Session, labels_service: LocalDbLabelsService
) -> None:
    """Verify that read_labels returns empty EntityLabels when no labels exist."""
    test_entity_uri = 'did:plc:testuser_empty'

    entity = MockEntity(test_entity_uri)
    result = labels_service.read_labels(entity)

    assert isinstance(result, EntityLabels)
    assert len(result.labels) == 0


def test_filters_by_labeler_did(
    sqlalchemy_session: Session, labels_service: LocalDbLabelsService
) -> None:
    """Verify that read_labels only returns labels from configured labeler DIDs."""
    now = datetime.now(timezone.utc)
    test_entity_uri = 'did:plc:testuser_filter'
    allowed_labeler = 'did:plc:e4elbtctnfqocyfcml6h2lf7'
    untrusted_labeler = 'did:plc:untrusted'

    # Insert label from allowed labeler
    allowed_label = LabelsModel(
        src=allowed_labeler,
        uri=test_entity_uri,
        val='allowed',
        cts=now,
        exp=None,
    )

    # Insert label from untrusted labeler
    untrusted_label = LabelsModel(
        src=untrusted_labeler,
        uri=test_entity_uri,
        val='untrusted',
        cts=now,
        exp=None,
    )

    sqlalchemy_session.add(allowed_label)
    sqlalchemy_session.add(untrusted_label)
    sqlalchemy_session.commit()

    # Call read_labels
    entity = MockEntity(test_entity_uri)
    result = labels_service.read_labels(entity)

    # Verify only allowed label is returned
    assert len(result.labels) == 1
    assert 'allowed' in result.labels
    assert 'untrusted' not in result.labels


def test_future_expiration_labels_are_included(
    sqlalchemy_session: Session, labels_service: LocalDbLabelsService
) -> None:
    """Verify that labels with future expiration are included."""
    now = datetime.now(timezone.utc)
    future = now + timedelta(hours=1)
    test_entity_uri = 'did:plc:testuser_future'
    test_labeler_did = 'did:plc:e4elbtctnfqocyfcml6h2lf7'

    # Insert label with future expiration
    future_label = LabelsModel(
        src=test_labeler_did,
        uri=test_entity_uri,
        val='future-expiring',
        cts=now,
        exp=future,
    )

    sqlalchemy_session.add(future_label)
    sqlalchemy_session.commit()

    # Call read_labels
    entity = MockEntity(test_entity_uri)
    result = labels_service.read_labels(entity)

    # Verify label with future expiration is included
    assert len(result.labels) == 1
    assert 'future-expiring' in result.labels
    assert result.labels['future-expiring'].status == LabelStatus.ADDED


def test_null_expiration_labels_are_included(
    sqlalchemy_session: Session, labels_service: LocalDbLabelsService
) -> None:
    """Verify that labels with NULL expiration (no expiry) are included."""
    now = datetime.now(timezone.utc)
    test_entity_uri = 'did:plc:testuser_null_exp'
    test_labeler_did = 'did:plc:e4elbtctnfqocyfcml6h2lf7'

    # Insert label with NULL expiration
    no_expiry_label = LabelsModel(
        src=test_labeler_did,
        uri=test_entity_uri,
        val='no-expiry',
        cts=now,
        exp=None,
    )

    sqlalchemy_session.add(no_expiry_label)
    sqlalchemy_session.commit()

    # Call read_labels
    entity = MockEntity(test_entity_uri)
    result = labels_service.read_labels(entity)

    # Verify label with NULL expiration is included
    assert len(result.labels) == 1
    assert 'no-expiry' in result.labels
    assert result.labels['no-expiry'].status == LabelStatus.ADDED


def test_read_modify_write_labels_atomically_add_label(
    sqlalchemy_session: Session, labels_service: LocalDbLabelsService
) -> None:
    """
    Test atomic read-modify-write: adding a new label.

    Verify that the context manager correctly yields EntityLabels,
    accepts mutations, and persists them to the local table.
    """
    now = datetime.now(timezone.utc)
    test_entity_uri = 'did:plc:testuser_atomic_add'
    test_labeler_did = 'did:plc:e4elbtctnfqocyfcml6h2lf7'

    entity = MockEntity(test_entity_uri)

    with labels_service.read_modify_write_labels_atomically(entity) as labels:
        # Verify initially empty
        assert len(labels.labels) == 0

        # Add a label
        labels.labels['new-label'] = MagicMock()

    # Verify label was persisted to local table
    sqlalchemy_session.refresh(sqlalchemy_session.query(LabelsModel).filter(
        LabelsModel.uri == test_entity_uri,
        LabelsModel.val == 'new-label',
    ).first() or LabelsModel())

    persisted = sqlalchemy_session.query(LabelsModel).filter(
        LabelsModel.uri == test_entity_uri,
        LabelsModel.val == 'new-label',
    ).first()

    assert persisted is not None
    assert persisted.src == test_labeler_did
    assert persisted.uri == test_entity_uri
    assert persisted.val == 'new-label'


def test_read_modify_write_labels_atomically_remove_label(
    sqlalchemy_session: Session, labels_service: LocalDbLabelsService
) -> None:
    """
    Test atomic read-modify-write: removing an existing label.

    Verify that removing a label from the EntityLabels dict
    results in deletion from the local table.
    """
    now = datetime.now(timezone.utc)
    test_entity_uri = 'did:plc:testuser_atomic_remove'
    test_labeler_did = 'did:plc:e4elbtctnfqocyfcml6h2lf7'

    # Pre-populate with a label
    label = LabelsModel(
        src=test_labeler_did,
        uri=test_entity_uri,
        val='to-remove',
        cts=now,
        exp=None,
    )
    sqlalchemy_session.add(label)
    sqlalchemy_session.commit()

    # Verify label exists before removal
    pre_remove = sqlalchemy_session.query(LabelsModel).filter(
        LabelsModel.uri == test_entity_uri,
        LabelsModel.val == 'to-remove',
    ).first()
    assert pre_remove is not None

    entity = MockEntity(test_entity_uri)

    with labels_service.read_modify_write_labels_atomically(entity) as labels:
        # Verify label was loaded
        assert 'to-remove' in labels.labels

        # Remove the label
        del labels.labels['to-remove']

    # Verify label was deleted from local table
    post_remove = sqlalchemy_session.query(LabelsModel).filter(
        LabelsModel.uri == test_entity_uri,
        LabelsModel.val == 'to-remove',
    ).first()

    assert post_remove is None


def test_read_modify_write_labels_atomically_mutation_persists(
    sqlalchemy_session: Session, labels_service: LocalDbLabelsService
) -> None:
    """
    Test atomic read-modify-write: complex mutations.

    Verify that adding, removing, and leaving unchanged labels
    all result in correct persistence.
    """
    now = datetime.now(timezone.utc)
    test_entity_uri = 'did:plc:testuser_atomic_complex'
    test_labeler_did = 'did:plc:e4elbtctnfqocyfcml6h2lf7'

    # Pre-populate with two labels
    label1 = LabelsModel(
        src=test_labeler_did,
        uri=test_entity_uri,
        val='keep-this',
        cts=now,
        exp=None,
    )
    label2 = LabelsModel(
        src=test_labeler_did,
        uri=test_entity_uri,
        val='delete-this',
        cts=now,
        exp=None,
    )
    sqlalchemy_session.add(label1)
    sqlalchemy_session.add(label2)
    sqlalchemy_session.commit()

    entity = MockEntity(test_entity_uri)

    with labels_service.read_modify_write_labels_atomically(entity) as labels:
        # Verify both labels were loaded
        assert len(labels.labels) == 2
        assert 'keep-this' in labels.labels
        assert 'delete-this' in labels.labels

        # Remove one label
        del labels.labels['delete-this']

        # Add a new label
        labels.labels['add-this'] = MagicMock()

    # Verify local table state
    kept = sqlalchemy_session.query(LabelsModel).filter(
        LabelsModel.uri == test_entity_uri,
        LabelsModel.val == 'keep-this',
    ).first()
    deleted = sqlalchemy_session.query(LabelsModel).filter(
        LabelsModel.uri == test_entity_uri,
        LabelsModel.val == 'delete-this',
    ).first()
    added = sqlalchemy_session.query(LabelsModel).filter(
        LabelsModel.uri == test_entity_uri,
        LabelsModel.val == 'add-this',
    ).first()

    assert kept is not None, "Unchanged label should be in table"
    assert deleted is None, "Removed label should not be in table"
    assert added is not None, "Added label should be in table"
