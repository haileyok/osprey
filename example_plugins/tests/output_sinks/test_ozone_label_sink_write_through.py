from __future__ import absolute_import

from datetime import datetime, timedelta, timezone
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest
from osprey.worker.lib.config import Config
from osprey.worker.lib.storage.postgres import scoped_session
from output_sinks.ozone_label_sink import OzoneLabelSink
from services.local_db_labels_service import LabelsModel
from sqlalchemy.orm.session import Session
from udfs.atproto.label import AtprotoLabelEffect


@pytest.fixture(autouse=True)
def sqlalchemy_session() -> Iterator[Session]:
    """Session fixture for integration tests with Postgres."""
    with scoped_session(database='osprey_db') as session:
        session.query(LabelsModel).filter(
            LabelsModel.uri.ilike('did:plc:test_write_through%')
        ).delete()
        session.commit()
        yield session
        session.query(LabelsModel).filter(
            LabelsModel.uri.ilike('did:plc:test_write_through%')
        ).delete()
        session.commit()


@pytest.fixture
def mock_config() -> Config:
    """Mock Config object with labeler DID."""
    config = MagicMock(spec=Config)
    config.get_str = MagicMock(side_effect=lambda key, default=None: {
        'OSPREY_BLUESKY_LABELER_DID': 'did:plc:e4elbtctnfqocyfcml6h2lf7',
    }.get(key, default))
    return config


@pytest.fixture
def mock_ozone_client() -> MagicMock:
    """Mock OzoneClient."""
    client = MagicMock()
    client.add_or_remove_label = MagicMock(return_value=None)
    return client


@pytest.fixture
def ozone_sink(mock_config: Config, mock_ozone_client: MagicMock) -> OzoneLabelSink:
    """OzoneLabelSink instance with mocked OzoneClient."""
    with patch('output_sinks.ozone_label_sink.OzoneClient') as MockOzoneClass:
        MockOzoneClass.get_instance = MagicMock(return_value=mock_ozone_client)
        sink = OzoneLabelSink(config=mock_config)
        sink._client = mock_ozone_client
        return sink


def test_ac5_1_labels_immediately_visible_after_write_through(
    sqlalchemy_session: Session, ozone_sink: OzoneLabelSink
) -> None:
    """
    AC5.1: Labels applied by Osprey via OzoneLabelSink are immediately visible to HasLabel.

    Verify that after calling _apply_label() with a successful Ozone write,
    the label appears in the local labels table immediately.
    """
    # Create a test label effect
    test_entity = 'did:plc:test_write_through_ac5_1'
    test_label = 'spam'
    test_cid = 'bafy123test'
    test_comment = 'Test label for AC5.1'

    effect = AtprotoLabelEffect(
        entity=test_entity,
        label=test_label,
        comment=test_comment,
        cid=test_cid,
        expiration_in_hours=None,
    )

    # Call _apply_label
    ozone_sink._apply_label(action_id=1, effect=effect)

    # Verify the Ozone write succeeded
    ozone_sink._client.add_or_remove_label.assert_called_once_with(
        action_id=1,
        entity_id=test_entity,
        label=test_label,
        neg=False,
        comment=test_comment,
        expiration_in_hours=None,
        cid=test_cid,
    )

    # Query the labels table to verify the write-through
    rows = sqlalchemy_session.query(LabelsModel).filter(
        LabelsModel.uri == test_entity,
        LabelsModel.val == test_label,
    ).all()

    # Assert the label exists
    assert len(rows) == 1, f"Expected 1 label row, got {len(rows)}"
    row = rows[0]
    assert row.uri == test_entity
    assert row.val == test_label
    assert row.cid == test_cid
    assert row.cts is not None
    assert isinstance(row.cts, datetime)


def test_ac5_2_correct_labeler_did_as_src(
    sqlalchemy_session: Session, ozone_sink: OzoneLabelSink
) -> None:
    """
    AC5.2: Write-through uses the correct labeler DID as src.

    Verify that the label row inserted by write-through has the correct
    src value matching OSPREY_BLUESKY_LABELER_DID.
    """
    # Create a test label effect
    test_entity = 'did:plc:test_write_through_ac5_2'
    test_label = 'nsfw'
    test_cid = 'bafy456test'
    test_comment = 'Test label for AC5.2'
    expected_labeler_did = 'did:plc:e4elbtctnfqocyfcml6h2lf7'

    effect = AtprotoLabelEffect(
        entity=test_entity,
        label=test_label,
        comment=test_comment,
        cid=test_cid,
        expiration_in_hours=None,
    )

    # Call _apply_label
    ozone_sink._apply_label(action_id=2, effect=effect)

    # Query the labels table
    rows = sqlalchemy_session.query(LabelsModel).filter(
        LabelsModel.uri == test_entity,
        LabelsModel.val == test_label,
    ).all()

    # Assert the label exists with correct src
    assert len(rows) == 1
    row = rows[0]
    assert row.src == expected_labeler_did, \
        f"Expected src={expected_labeler_did}, got src={row.src}"


def test_ac5_3_write_through_failure_does_not_block_ozone_write(
    ozone_sink: OzoneLabelSink
) -> None:
    """
    AC5.3: Write-through failure does not block the Ozone HTTP write (best-effort).

    Verify that when the local database write fails, _apply_label() still
    completes successfully and the Ozone write is recorded.
    """
    # Create a test label effect
    test_entity = 'did:plc:test_write_through_ac5_3'
    test_label = 'spam'
    test_cid = 'bafy789test'
    test_comment = 'Test label for AC5.3'

    effect = AtprotoLabelEffect(
        entity=test_entity,
        label=test_label,
        comment=test_comment,
        cid=test_cid,
        expiration_in_hours=None,
    )

    # Mock scoped_session to raise an exception
    with patch('output_sinks.ozone_label_sink.scoped_session') as mock_session:
        mock_session.side_effect = Exception('Database connection failed')

        # Call _apply_label - should NOT raise
        try:
            ozone_sink._apply_label(action_id=3, effect=effect)
        except Exception as e:
            pytest.fail(f"_apply_label should not raise, but raised {e}")

    # Verify the Ozone write still succeeded
    ozone_sink._client.add_or_remove_label.assert_called_once_with(
        action_id=3,
        entity_id=test_entity,
        label=test_label,
        neg=False,
        comment=test_comment,
        expiration_in_hours=None,
        cid=test_cid,
    )


def test_write_through_with_expiration(
    sqlalchemy_session: Session, ozone_sink: OzoneLabelSink
) -> None:
    """Verify that write-through correctly computes expiration time."""
    test_entity = 'did:plc:test_write_through_exp'
    test_label = 'sensitive'
    test_cid = 'bafyexp123test'
    test_comment = 'Test label with expiration'
    expiration_hours = 24

    effect = AtprotoLabelEffect(
        entity=test_entity,
        label=test_label,
        comment=test_comment,
        cid=test_cid,
        expiration_in_hours=expiration_hours,
    )

    before_call = datetime.now(timezone.utc)
    ozone_sink._apply_label(action_id=4, effect=effect)
    after_call = datetime.now(timezone.utc)

    # Query the labels table
    rows = sqlalchemy_session.query(LabelsModel).filter(
        LabelsModel.uri == test_entity,
        LabelsModel.val == test_label,
    ).all()

    assert len(rows) == 1
    row = rows[0]
    assert row.exp is not None

    # Verify expiration is approximately 24 hours from now
    expected_min = before_call + timedelta(hours=expiration_hours)
    expected_max = after_call + timedelta(hours=expiration_hours)
    assert expected_min <= row.exp <= expected_max, \
        f"Expiration {row.exp} should be within 24 hours from now"


def test_write_through_without_labeler_did(
    mock_config: Config, mock_ozone_client: MagicMock
) -> None:
    """Verify that write-through is skipped when labeler DID is not configured."""
    # Create config without labeler DID
    config = MagicMock(spec=Config)
    config.get_str = MagicMock(return_value='')

    with patch('output_sinks.ozone_label_sink.OzoneClient') as MockOzoneClass:
        MockOzoneClass.get_instance = MagicMock(return_value=mock_ozone_client)
        sink = OzoneLabelSink(config=config)
        sink._client = mock_ozone_client

    effect = AtprotoLabelEffect(
        entity='did:plc:test_no_labeler',
        label='spam',
        comment='Test without labeler DID',
        cid='bafy_no_labeler',
        expiration_in_hours=None,
    )

    # Call _apply_label - should complete without attempting write-through
    sink._apply_label(action_id=5, effect=effect)

    # Verify the Ozone write still succeeded
    sink._client.add_or_remove_label.assert_called_once()


def test_write_through_upsert_idempotency(
    sqlalchemy_session: Session, ozone_sink: OzoneLabelSink
) -> None:
    """
    Verify that calling _apply_label multiple times with the same effect
    results in the row being updated, not duplicated (idempotent upsert).
    """
    test_entity = 'did:plc:test_write_through_idempotent'
    test_label = 'duplicate'
    test_cid_v1 = 'bafyv1_123'
    test_cid_v2 = 'bafyv2_456'
    test_comment = 'Test idempotent upsert'

    # First call with v1 CID
    effect_v1 = AtprotoLabelEffect(
        entity=test_entity,
        label=test_label,
        comment=test_comment,
        cid=test_cid_v1,
        expiration_in_hours=None,
    )
    ozone_sink._apply_label(action_id=6, effect=effect_v1)

    rows = sqlalchemy_session.query(LabelsModel).filter(
        LabelsModel.uri == test_entity,
        LabelsModel.val == test_label,
    ).all()
    assert len(rows) == 1
    assert rows[0].cid == test_cid_v1

    # Second call with v2 CID
    effect_v2 = AtprotoLabelEffect(
        entity=test_entity,
        label=test_label,
        comment=test_comment,
        cid=test_cid_v2,
        expiration_in_hours=None,
    )
    ozone_sink._apply_label(action_id=7, effect=effect_v2)

    rows = sqlalchemy_session.query(LabelsModel).filter(
        LabelsModel.uri == test_entity,
        LabelsModel.val == test_label,
    ).all()

    # Should still be 1 row, with updated CID
    assert len(rows) == 1, f"Expected 1 row (upserted), got {len(rows)}"
    assert rows[0].cid == test_cid_v2
