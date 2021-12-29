"""Create a materialized view of MLS data joined to all other entities."""
from wheretolive.postgis import PostGIS

SQL = """
    DROP MATERIALIZED VIEW IF EXISTS rfaster_wide CASCADE;
    CREATE MATERIALIZED VIEW rfaster_wide AS
    SELECT rfaster.*, rfaster_commutes.*, rfaster_grocery.*
    FROM rfaster JOIN rfaster_commutes ON rfaster.rfaster_id = rfaster_commutes.rfaster_commute_id JOIN rfaster_grocery ON rfaster.rfaster_id = rfaster_grocery.rfaster_grocery_id;
"""

with PostGIS().connection.begin() as conn:
    conn.execute(SQL)
