-- Create tables for the raw mls data
CREATE TABLE IF NOT EXISTS public.mls_staging (
  mls_id INTEGER NOT NULL,
  mls_number CHARACTER(8) NOT NULL,
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
  longitude NUMERIC(14, 10),
  latitude NUMERIC(14, 10),
  ownership_type VARCHAR(50),
  parking VARCHAR(300),
  lot_size VARCHAR(300),
  postal_code CHARACTER(6),
  link VARCHAR(200),
  price_change_dt DATE,
  scrape_date DATE
);

CREATE OR REPLACE VIEW mls_insert_stage_view AS (
    SELECT
        mls_id,
        mls_number,
        listing_description,
        bedrooms_above,
        bedrooms_below,
        bedrooms,
        bathrooms,
        sq_feet_in,
        listing_type,
        amenities,
        price AS current_price,
        price AS lowest_price,
        price AS highest_price,
        property_type,
        listing_address,
        ST_MakePoint(longitude, latitude) AS latlong,
        ownership_type,
        parking,
        lot_size,
        postal_code,
        link,
        scrape_date AS first_scrape_date,
        scrape_date AS latest_scrape_date
    FROM mls_staging
    WHERE mls_id NOT IN (SELECT DISTINCT mls_id FROM mls)
);

CREATE OR REPLACE VIEW mls_update_stage_view AS (
    SELECT
        mls_staging.mls_id,
        mls_staging.mls_number,
        mls_staging.listing_description,
        mls_staging.bedrooms_above,
        mls_staging.bedrooms_below,
        mls_staging.bedrooms,
        mls_staging.bathrooms,
        mls_staging.sq_feet_in,
        mls_staging.listing_type,
        mls_staging.amenities,
        mls_staging.price AS current_price,
        LEAST(mls_staging.price, mls.lowest_price) AS lowest_price,
        GREATEST(mls_staging.price, mls.highest_price) AS highest_price,
        mls_staging.property_type,
        mls_staging.listing_address,
        ST_MakePoint(mls_staging.longitude, mls_staging.latitude) AS latlong,
        mls_staging.ownership_type,
        mls_staging.parking,
        mls_staging.lot_size,
        mls_staging.postal_code,
        mls_staging.link,
        mls_staging.scrape_date AS latest_scrape_date
    FROM mls_staging
    INNER JOIN mls ON mls_staging.mls_id = mls.mls_id
    WHERE mls_staging.scrape_date > mls.latest_scrape_date
);

CREATE OR REPLACE VIEW mls_update_backfill_stage_view AS (
    SELECT
        mls_staging.mls_id,
        LEAST(mls_staging.price, mls.lowest_price) AS lowest_price,
        GREATEST(mls_staging.price, mls.highest_price) AS highest_price,
        LEAST(mls_staging.scrape_date, mls.first_scrape_date) AS first_scrape_date
    FROM mls_staging
    INNER JOIN mls ON mls_staging.mls_id = mls.mls_id
    WHERE mls_staging.scrape_date <= mls.latest_scrape_date
);
