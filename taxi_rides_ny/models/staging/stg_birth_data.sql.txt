SELECT
  unique_id,
  CAST(dob_yy AS INT64) AS birth_year,  -- Explicit type casting
  PARSE_DATE('%Y-%m-%d', dob_date) AS birth_date,  -- Ensure proper date type
  CAST(dbwt AS INT64) AS birth_weight_grams,
  CAST(mager AS INT64) AS mother_age,
  CAST(mager9 AS INT64) AS mother_age_group
FROM {{ source('natality', 'natality_data') }}  -- Matches source config
WHERE dob_date IS NOT NULL  -- Basic data quality filter