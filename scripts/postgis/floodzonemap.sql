-- Create views mapping rentfaster and mls listings to flood maps
DROP VIEW IF EXISTS rfaster_flood;
CREATE OR REPLACE VIEW rfaster_flood AS
WITH
  cte_100_flood
  AS
  (
    SELECT geom
    FROM floodmap
    WHERE scenario = '1 in 100 chance flood'
  ),
  cte_20_flood
  AS
  (
    SELECT geom
    FROM floodmap
    WHERE scenario = '1 in 20 chance flood'
  )
SELECT
  rfaster.rfaster_id AS rfaster_flood_id,
  CASE
    WHEN ST_Contains(cte_20_flood.geom, rfaster.geom)
    THEN '1 in 20 flood risk'
    WHEN ST_Contains(cte_100_flood.geom, rfaster.geom)
THEN '1 in 100 flood risk'
    ELSE 'Outside flood map'
END AS flood_risk
FROM rfaster, cte_20_flood, cte_100_flood
;
DROP VIEW IF EXISTS mls_flood;
CREATE OR REPLACE VIEW mls_flood AS
WITH
  cte_100_flood
  AS
  (
    SELECT geom
    FROM floodmap
    WHERE scenario = '1 in 100 chance flood'
  ),
  cte_20_flood
  AS
  (
    SELECT geom
    FROM floodmap
    WHERE scenario = '1 in 20 chance flood'
  )
SELECT
  mls.mls_id AS mls_flood_id,
  CASE
    WHEN ST_Contains(cte_20_flood.geom, mls.geom)
    THEN '1 in 20 flood risk'
    WHEN ST_Contains(cte_100_flood.geom, mls.geom)
THEN '1 in 100 flood risk'
    ELSE 'Outside flood map'
END AS flood_risk
FROM mls, cte_20_flood, cte_100_flood
;
