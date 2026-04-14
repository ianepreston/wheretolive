-- Drop the mls record and staging tables. Useful for a hard reset while developing
DROP MATERIALIZED VIEW IF EXISTS mls_wide CASCADE;
DROP VIEW IF EXISTS mls_flood;
DROP VIEW IF EXISTS mls_commutes;
DROP VIEW IF EXISTS mls_grocery;
DROP TABLE IF EXISTS mls;
