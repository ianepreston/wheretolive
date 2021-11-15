"""Functionality for the PostGIS database I'm using to store data."""
import os

import sqlalchemy  # type: ignore
from dotenv import load_dotenv

load_dotenv()

PG_PASSWORD = os.getenv("POSTGIS_PASS")


class _DB:
    """Base class for sql database connections."""

    # Singleton pattern for metadata
    # avoids name conflicts between multiple instantiations of metadata
    _meta = sqlalchemy.MetaData()

    def __init__(self) -> None:
        """Create a database connection."""
        self._connect_string = None

    @property
    def connection(self) -> sqlalchemy.engine.Engine:
        """Get the connection to the DB.

        Returns
        -------
        sqlalchemy.engine.Engine
            The connection object
        """
        return sqlalchemy.create_engine(self._connect_string)

    def reflect_full_db(self) -> sqlalchemy.MetaData:
        """Load metadata about the entire database into the object.

        Note that this can take a very long time for big databases.
        For example, for the data warehouse, expect to wait several minutes.
        Some databases, such as PeopleSoft have tens of thousands of tables, and
        using this function against them will take hours and consume significant amounts
        of memory. In most situations you will not have to use this method, and can
        simply use the ``get_table`` method to return metadata for a specific table.

        Returns
        -------
        sqlalchemy.schema.MetaData
            A metadata object with complete information of the database
        """
        db: sqlalchemy.engine.Engine = self.connection
        meta: sqlalchemy.MetaData = sqlalchemy.MetaData()
        meta.reflect(bind=db, views=True)
        self.__class__._meta = meta
        return meta

    @property
    def metadata(self) -> sqlalchemy.MetaData:
        """Get the metadata object associated with the database.

        Returns
        -------
        sqlalchemy.schema.MetaData
            A metadata object for the database, contains objects for tables in the
            database, which in turn contains objects for things like their columns
        """
        return self.__class__._meta

    def get_table(self, table_name: str) -> sqlalchemy.Table:
        """Get a table object from the database.

        Parameters
        ----------
        table_name: str
            The name of the table to retrieve

        Returns
        -------
        sqlalchemy.Table:
            The associated table object
        """
        return sqlalchemy.Table(
            table_name, self.metadata, autoload_with=self.connection
        )


class PostGIS(_DB):
    """My Wheretolive PostGIS database."""

    def __init__(self) -> None:
        super().__init__()
        self._connect_string = f"postgresql://postgres:{PG_PASSWORD}@mars:5432/postgres"
