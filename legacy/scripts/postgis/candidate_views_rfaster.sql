-- Candidate Views of MLS listings
DROP VIEW IF EXISTS ian_candidates_rfaster;
CREATE OR REPLACE VIEW ian_candidates_rfaster AS
SELECT
  link,
  price,
  first_seen_dt,
  scrape_dt,
  flood_risk,
  downtown_walk_time,
  downtown_walk_transit_time,
  gf_work_car_time,
  brother_car_time,
  grocery_store_name,
  m_to_grocery,
  listing_description
FROM rfaster_wide
WHERE
  price BETWEEN 1900 AND 3500
  AND ((bedrooms >= 2 AND den = TRUE) OR (bedrooms >= 3))
  AND gf_work_car_30 = TRUE
  AND brother_car_40 = TRUE
  AND bathrooms >= 2
  AND downtown_walk_transit_40 = TRUE
  AND rented = FALSE
  AND dogs = TRUE
  AND smoking = 'Non-Smoking'
  AND garage_size != 'street parking'
ORDER BY first_seen_dt DESC
;
