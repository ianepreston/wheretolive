-- Candidate Views of MLS listings
DROP VIEW IF EXISTS ian_candidates;
CREATE OR REPLACE VIEW ian_candidates AS
SELECT
  link,
  price,
  mls_insert_dt,
  price_change_dt,
  scrape_dt,
  downtown_walk_time,
  downtown_walk_transit_time,
  gf_work_car_time,
  brother_car_time,
  to_bc_car_time,
  listing_description
FROM mls_wide
WHERE
  price BETWEEN 300000 AND 650000
  AND bedrooms >= 2
  AND gf_work_car_30 = TRUE
  AND brother_car_40 = TRUE
  AND bathrooms >= 2
  AND (
      parking LIKE '%Garage%'
      OR parking LIKE '%Underground%'
  )
  AND listing_type NOT IN ('Duplex', 'Mobile Home')
  AND downtown_walk_transit_40
ORDER BY GREATEST(mls_insert_dt, price_change_dt) DESC
;
DROP VIEW IF EXISTS parents_candidates;
CREATE OR REPLACE VIEW parents_candidates AS
SELECT
  link,
  price,
  mls_insert_dt,
  price_change_dt,
  scrape_dt,
  brother_car_time,
  downtown_car_time,
  gf_work_car_time,
  listing_description
  FROM mls_wide
  WHERE
    price BETWEEN 300000 AND 550000
    AND sq_feet_in >= 1000
    AND bathrooms >= 2
    AND bedrooms_above >= 2
    AND bedrooms <= 3
    AND (
        parking LIKE '%Garage%'
        OR parking LIKE '%Underground%'
    )
    AND brother_car_20 = TRUE
    AND (
      (listing_type != 'Apartment' AND stories < 3)
      OR (listing_type = 'Apartment')
    )
  ORDER BY GREATEST(mls_insert_dt, price_change_dt) DESC
;
DROP VIEW IF EXISTS jill_candidates;
CREATE OR REPLACE VIEW jill_candidates AS
SELECT
  mls_number,
  link,
  price,
  mls_insert_dt,
  price_change_dt,
  scrape_dt,
  listing_description,
  downtown_walk_time,
  downtown_walk_transit_time,
  downtown_car_time,
  to_bc_car_time,
  momma_jill_car_time
FROM mls_wide
WHERE
  price BETWEEN 900000 AND 1500000
  AND bedrooms_above >= 3
  AND bathrooms > 3
  AND parking_spaces >= 2
  AND (
    (downtown_car_25 = TRUE)
    OR (downtown_walk_30 = TRUE)
  )
  AND (
    (listing_type = 'House')
    OR (listing_type is NULL)
  )
  AND momma_jill_car_10 = False
  AND sq_feet_in >= 2000
ORDER BY GREATEST(mls_insert_dt, price_change_dt) DESC
;
