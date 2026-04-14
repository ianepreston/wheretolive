-- Create tables for the raw rentfaster data
CREATE TABLE IF NOT EXISTS public.rfaster (
  rfaster_id VARCHAR(10) NOT NULL,
  price REAL,
  listing_description VARCHAR(5000),
  sq_feet_in REAL,
  avdate TIMESTAMP,
  link VARCHAR(200),
  rented BOOLEAN,
  smoking VARCHAR(20),
  lease_term VARCHAR(50),
  garage_size VARCHAR(100),
  bedrooms SMALLINT,
  den BOOLEAN,
  bathrooms SMALLINT,
  cats BOOLEAN,
  dogs BOOLEAN,
  electricity BOOLEAN,
  water BOOLEAN,
  heat BOOLEAN,
  internet BOOLEAN,
  cable BOOLEAN,
  util_check_listing BOOLEAN,
  scrape_dt TIMESTAMP,
  first_seen_dt TIMESTAMP
);

SELECT
AddGeometryColumn('public', 'rfaster','geom',4326,'POINT',2);
