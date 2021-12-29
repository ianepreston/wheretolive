"""Create the database and schemas necessary for."""
from pathlib import Path

from wheretolive.postgis import PostGIS

WORKDIR = Path(__file__).resolve().parent


def exec_sql(query_file: str):
    """Run a sql script saved in this folder."""
    pg_db = PostGIS().connection
    with open(WORKDIR / query_file, "r") as f:
        sql = f.read()
    with pg_db.begin() as conn:
        conn.execute(sql)


def drop_mls():
    """Drop the MLS tables. Only call this for a hard reset."""
    exec_sql("drop_mls.sql")


def create_mls():
    """Create the final databse for MLS scrapes."""
    exec_sql("create_mls.sql")


def create_rfaster():
    """Create the final database from rfaster scrapes."""
    exec_sql("create_rfaster.sql")


if __name__ == "__main__":
    # drop_mls()
    # create_mls()
    # create_rfaster()
    exec_sql("grocery_distance.sql")
