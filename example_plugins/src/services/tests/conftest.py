from typing import Iterator

import pytest
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.storage.postgres import init_from_config
from psycopg2.errors import DuplicateDatabase
from sqlalchemy.exc import ProgrammingError
from sqlalchemy_utils import create_database, drop_database


def make_ozone_database_config_fixture() -> object:
    """Returns a fixture which sets up the Ozone test database for the session."""

    @pytest.fixture(scope='session', autouse=True)
    def ozone_database_config() -> Iterator[None]:
        config = CONFIG.instance()
        config.configure_from_env()

        try:
            url = config['POSTGRES_HOSTS'].get('ozone_db')
        except (KeyError, AttributeError):
            url = None

        if url is None:
            pytest.fail('POSTGRES_HOSTS.ozone_db not configured')

        try:
            create_database(url)
        except ProgrammingError as e:
            # If the database already exists, we're chill and have nothing left to do!
            if not isinstance(e.orig, DuplicateDatabase):
                raise

        init_from_config('ozone_db')

        config.unconfigure_for_tests()

        yield

        try:
            drop_database(url)
        except ProgrammingError as e:
            # Don't fail if the database is already closed
            from psycopg2.errors import InvalidCatalogName

            if not isinstance(e.orig, InvalidCatalogName):
                raise

    return ozone_database_config


ozone_database_config = make_ozone_database_config_fixture()
