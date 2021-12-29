"""Create a materialized view of MLS data joined to all other entities."""
from wheretolive.postgis import PostGIS

SQL = """
    DROP MATERIALIZED VIEW IF EXISTS rfaster_wide;
    CREATE MATERIALIZED VIEW rfaster_wide AS
    SELECT rfaster.*, rfaster_commutes.*
    FROM rfaster JOIN rfaster_commutes ON rfaster.rfaster_id = rfaster_commutes.rfaster_commute_id;
"""

with PostGIS().connection.begin() as conn:
    conn.execute(SQL)
