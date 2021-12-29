"""Create a materialized view of MLS data joined to all other entities."""
from wheretolive.postgis import PostGIS

SQL = """
    DROP MATERIALIZED VIEW IF EXISTS mls_wide CASCADE;
    CREATE MATERIALIZED VIEW mls_wide AS
    SELECT mls.*, mls_commutes.*, mls_grocery.*
    FROM mls JOIN mls_commutes ON mls.mls_id = mls_commutes.mls_commute_id JOIN mls_grocery ON mls.mls_id = mls_grocery.mls_grocery_id;
"""

with PostGIS().connection.begin() as conn:
    conn.execute(SQL)
