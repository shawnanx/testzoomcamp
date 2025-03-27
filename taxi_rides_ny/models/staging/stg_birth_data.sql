SELECT
  unique_id,
  dob_yy AS birth_year,
  dob_date AS birth_date,
  dbwt AS birth_weight_grams,
  mager AS mother_age,
  mager9 AS mother_age_group
FROM {{ source('natality', 'birth_data') }}