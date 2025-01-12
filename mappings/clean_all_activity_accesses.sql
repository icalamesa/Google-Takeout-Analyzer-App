CREATE OR REPLACE TABLE clean_all_activity_accesses AS
SELECT
    distinct *
FROM raw_all_activity_accesses;
