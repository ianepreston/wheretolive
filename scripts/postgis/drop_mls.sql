-- Drop the mls record and staging tables. Useful for a hard reset while developing
DROP TABLE IF EXISTS mls;
DROP TABLE IF EXISTS mls_staging;
DROP VIEW IF EXISTS mls_update_stage_view;
DROP VIEW IF EXISTS mls_update_backfill_stage_view;
