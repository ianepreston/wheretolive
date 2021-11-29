-- Candidate Views of MLS listings
DROP VIEW IF EXISTS ian_candidates;
CREATE OR REPLACE VIEW ian_candidates AS
SELECT
  mls_number,
  link,
  price,
  mls_insert_dt,
  scrape_dt,
  listing_description,
  downtown_walk_time,
  gf_work_car_time,
  brother_car_time
FROM mls_wide
WHERE
  price BETWEEN 300000 AND 650000
  AND bedrooms >= 2
  AND gf_work_car_30 = TRUE
  AND brother_car_40 = TRUE
;
