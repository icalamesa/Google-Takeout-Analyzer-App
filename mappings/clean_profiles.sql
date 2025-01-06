CREATE OR REPLACE TABLE clean_profiles AS
SELECT
    raw_profiles['name']['givenName'] AS GivenName,
    raw_profiles['name']['formattedName'] AS FormattedName,
    raw_profiles['displayName'] AS DisplayName,
    raw_profiles['emails'][0]['value'] AS Email,
    raw_profiles['gender']['type'] AS GenderType
FROM raw_profiles;
