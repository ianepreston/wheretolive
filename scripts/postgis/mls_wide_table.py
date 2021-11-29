"""Create a materialized view of MLS data joined to all other entities."""
from wheretolive.postgis import PostGIS

SQL = """
    DROP MATERIALIZED VIEW IF EXISTS mls_wide;
    CREATE MATERIALIZED VIEW mls_wide AS
    SELECT mls.*, mls_commutes.*
    FROM mls JOIN mls_commutes ON mls.mls_id = mls_commutes.mls_commute_id;
"""

with PostGIS().connection.begin() as conn:
    conn.execute(SQL)
