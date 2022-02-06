-- Create tables for the raw mls data
CREATE TABLE IF NOT EXISTS public.mls (
  mls_id INTEGER NOT NULL PRIMARY KEY,
  mls_number CHARACTER(8) NOT NULL,
  stories INTEGER,
  listing_description VARCHAR(5000),
  bedrooms_above SMALLINT,
  bedrooms_below SMALLINT,
  bedrooms SMALLINT,
  bathrooms SMALLINT,
  sq_feet_in REAL,
  listing_type VARCHAR(50),
  amenities VARCHAR(300),
  price REAL,
  property_type VARCHAR(50),
  listing_address VARCHAR(200),
  longitude REAL,
  latitude REAL,
  ownership_type VARCHAR(50),
  parking VARCHAR(300),
  parking_spaces INTEGER,
  lot_size VARCHAR(300),
  postal_code CHARACTER(6),
  link VARCHAR(200),
  price_change_dt TIMESTAMP,
  mls_insert_dt TIMESTAMP,
  scrape_dt TIMESTAMP
);

SELECT
AddGeometryColumn('public','mls','geom',4326,'POINT',2)
