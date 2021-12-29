-- Create views of distance to nearest grocery stores for MLS and rentfasters
DROP VIEW IF EXISTS rfaster_grocery;
CREATE OR REPLACE VIEW rfaster_grocery AS
SELECT
  rfaster.rfaster_id AS rfaster_grocery_id,
  grocery.name AS grocery_store_name,
  grocery.dist AS m_to_grocery
FROM rfaster
CROSS JOIN LATERAL (
SELECT
  grocery_stores.name,
  ST_DistanceSphere(rfaster.geom, grocery_stores.geom) AS dist
FROM grocery_stores
ORDER BY dist
		LIMIT 1
) grocery;

DROP VIEW IF EXISTS mls_grocery;
CREATE OR REPLACE VIEW mls_grocery AS
SELECT
  mls.mls_id AS mls_grocery_id,
  grocery.name AS grocery_store_name,
  grocery.dist AS m_to_grocery
FROM mls
CROSS JOIN LATERAL (
SELECT
  grocery_stores.name,
  ST_DistanceSphere(mls.geom, grocery_stores.geom) AS dist
FROM grocery_stores
ORDER BY dist
		LIMIT 1
) grocery;
