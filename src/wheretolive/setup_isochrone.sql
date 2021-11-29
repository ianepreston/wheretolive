--- Create isochrone table for commute times
DROP TABLE IF EXISTS public.isochrones;

CREATE TABLE IF NOT EXISTS public.isochrones (
  place_name VARCHAR(50),
  commute_mode VARCHAR(50),
  cutoff_time INT,
  geom geometry(MultiPolygon, 4326)
);
