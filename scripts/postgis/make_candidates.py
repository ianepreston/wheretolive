"""Run the candidate views SQL script to create views of candidate MLS listings."""
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


exec_sql("candidate_views.sql")
