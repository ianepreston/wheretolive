-- Drop the mls record and staging tables. Useful for a hard reset while developing
DROP VIEW IF EXISTS mls_update_stage_view;
DROP VIEW IF EXISTS mls_insert_stage_view;
DROP VIEW IF EXISTS mls_update_backfill_stage_view;
DROP TABLE IF EXISTS mls_staging;
DROP TABLE IF EXISTS mls;
