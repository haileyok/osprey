import pytest
from osprey.worker.lib.storage.postgres import metadata as shared_metadata
from osprey.worker.lib.storage.postgres import scoped_session
from services.ozone_label_model import OzoneLabelModel, ozone_metadata
from sqlalchemy import insert


class TestOzoneLabelModelStructure:
    """Test ozone-direct-db.AC1.1: Model maps all columns of Ozone's label table."""

    def test_model_maps_all_columns(self):
        """Verify OzoneLabelModel has all 8 required columns."""
        columns = {c.name for c in OzoneLabelModel.__table__.columns}
        expected_columns = {'id', 'src', 'uri', 'cid', 'val', 'neg', 'cts', 'exp'}
        assert columns == expected_columns, f'Expected columns {expected_columns}, got {columns}'

    def test_model_table_name(self):
        """Verify OzoneLabelModel maps to the 'label' table."""
        assert OzoneLabelModel.__tablename__ == 'label'

    def test_primary_key(self):
        """Verify 'id' is the primary key."""
        pk_columns = {c.name for c in OzoneLabelModel.__table__.primary_key.columns}
        assert pk_columns == {'id'}

    def test_column_types(self):
        """Verify column types match the schema."""
        from sqlalchemy import BigInteger, Boolean, String

        columns_by_name = {c.name: c for c in OzoneLabelModel.__table__.columns}

        assert isinstance(columns_by_name['id'].type, BigInteger)
        assert isinstance(columns_by_name['src'].type, String)
        assert isinstance(columns_by_name['uri'].type, String)
        assert isinstance(columns_by_name['cid'].type, String)
        assert isinstance(columns_by_name['val'].type, String)
        assert isinstance(columns_by_name['neg'].type, Boolean)
        assert isinstance(columns_by_name['cts'].type, String)
        assert isinstance(columns_by_name['exp'].type, String)

    def test_column_nullability(self):
        """Verify column nullability is correct."""
        columns_by_name = {c.name: c for c in OzoneLabelModel.__table__.columns}

        # Not nullable: id, src, uri, cid, val, cts
        assert not columns_by_name['id'].nullable
        assert not columns_by_name['src'].nullable
        assert not columns_by_name['uri'].nullable
        assert not columns_by_name['cid'].nullable
        assert not columns_by_name['val'].nullable
        assert not columns_by_name['cts'].nullable

        # Nullable: neg, exp
        assert columns_by_name['neg'].nullable
        assert columns_by_name['exp'].nullable


class TestOzoneLabelModelIsolation:
    """Test ozone-direct-db.AC1.2: Model uses separate MetaData (not in shared metadata)."""

    def test_label_not_in_shared_metadata(self):
        """Verify 'label' table is NOT in the shared metadata."""
        assert 'label' not in shared_metadata.tables, (
            'label table should NOT be in shared metadata to prevent DDL on osprey_db'
        )

    def test_label_in_ozone_metadata(self):
        """Verify 'label' table IS in the ozone_metadata."""
        assert 'label' in ozone_metadata.tables, 'label table should be in ozone_metadata for Ozone database operations'

    def test_ozone_model_uses_separate_base(self):
        """Verify OzoneLabelModel does not inherit from the shared Model base."""
        # Import the shared Model to verify inheritance
        from osprey.worker.lib.storage.postgres import Model

        # OzoneLabelModel should NOT inherit from the shared Model
        assert OzoneLabelModel not in Model.registry.mappers


class TestOzoneLabelModelDatabaseConnection:
    """Test ozone-direct-db.AC1.2: init_from_config establishes connection."""

    def test_can_query_seeded_rows(self):
        """Verify seeded rows can be queried via scoped_session."""
        # Ensure ozone_metadata tables are created in the test database
        from osprey.worker.lib.singletons import CONFIG

        config = CONFIG.instance()
        try:
            url = config['POSTGRES_HOSTS'].get('ozone_db')
        except (KeyError, AttributeError):
            pytest.skip('POSTGRES_HOSTS.ozone_db not configured')

        if url is None:
            pytest.skip('POSTGRES_HOSTS.ozone_db not configured')

        # Get engine from the ozone_db session
        with scoped_session(database='ozone_db') as session:
            engine = session.get_bind()

            # Create the label table using ozone_metadata (not shared metadata)
            ozone_metadata.create_all(engine)

            try:
                # Seed test data
                test_rows = [
                    {
                        'id': 1,
                        'src': 'atproto:moderation/c',
                        'uri': 'at://did:plc:test/app.bsky.feed.post/123',
                        'cid': 'bafy1234',
                        'val': 'porn',
                        'neg': False,
                        'cts': '2026-02-21T00:00:00.000Z',
                        'exp': None,
                    },
                    {
                        'id': 2,
                        'src': 'atproto:moderation/c',
                        'uri': 'at://did:plc:test/app.bsky.feed.post/456',
                        'cid': 'bafy5678',
                        'val': 'spam',
                        'neg': True,
                        'cts': '2026-02-21T00:00:00.000Z',
                        'exp': '2026-03-21T00:00:00.000Z',
                    },
                ]

                for row in test_rows:
                    stmt = insert(OzoneLabelModel).values(**row)
                    session.execute(stmt)
                session.commit()

                # Query and verify the seeded data
                from sqlalchemy import select

                stmt = select(OzoneLabelModel).order_by(OzoneLabelModel.id)
                results = session.scalars(stmt).all()

                assert len(results) == 2
                assert results[0].id == 1
                assert results[0].src == 'atproto:moderation/c'
                assert results[0].uri == 'at://did:plc:test/app.bsky.feed.post/123'
                assert results[0].cid == 'bafy1234'
                assert results[0].val == 'porn'
                assert results[0].neg is False
                assert results[0].cts == '2026-02-21T00:00:00.000Z'
                assert results[0].exp is None

                assert results[1].id == 2
                assert results[1].src == 'atproto:moderation/c'
                assert results[1].uri == 'at://did:plc:test/app.bsky.feed.post/456'
                assert results[1].cid == 'bafy5678'
                assert results[1].val == 'spam'
                assert results[1].neg is True
                assert results[1].cts == '2026-02-21T00:00:00.000Z'
                assert results[1].exp == '2026-03-21T00:00:00.000Z'

            finally:
                # Cleanup: drop the table
                ozone_metadata.drop_all(engine)
