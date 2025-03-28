{{
  config(
    materialized='view'
  )
}}

WITH earliest_year_data AS (
  SELECT 
    MIN(CAST(dob_yy AS INT64)) AS earliest_year  -- Find earliest year
  FROM {{ source('natality', 'natality_data') }}
  WHERE dob_yy IS NOT NULL
),

age_distribution AS (
  SELECT
    CAST(mager9 AS INT64) AS age_group,
    COUNT(*) AS birth_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
  FROM {{ source('natality', 'natality_data') }}, earliest_year_data
  WHERE
    CAST(dob_yy AS INT64) = earliest_year_data.earliest_year  -- Filter for earliest year
    AND mager9 IS NOT NULL
    AND CAST(mager9 AS INT64) BETWEEN 1 AND 9  -- Data validation
  GROUP BY age_group
)

SELECT
  age_group,
  CASE age_group
    WHEN 1 THEN 'Under 15'
    WHEN 2 THEN '15-19'
    WHEN 3 THEN '20-24'
    WHEN 4 THEN '25-29'
    WHEN 5 THEN '30-34'
    WHEN 6 THEN '35-39'
    WHEN 7 THEN '40-44'
    WHEN 8 THEN '45-49'
    WHEN 9 THEN '50+'
    ELSE 'Unknown'
  END AS age_group_description,
  birth_count,
  percentage,
  (SELECT earliest_year FROM earliest_year_data) AS report_year  -- Show the earliest year
FROM age_distribution
ORDER BY age_group