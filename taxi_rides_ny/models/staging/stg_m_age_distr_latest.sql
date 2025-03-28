{{
  config(
    materialized='view'
  )
}}

WITH latest_year_data AS (
  SELECT 
    MAX(CAST(dob_yy AS INT64)) AS latest_year  -- Explicitly cast to INT
  FROM {{ source('natality', 'natality_data') }}
  WHERE dob_yy IS NOT NULL
),

age_distribution AS (
  SELECT
    CAST(mager9 AS INT64) AS age_group,  -- Ensure consistent type
    COUNT(*) AS birth_count,  -- Fixed column name (was 'count')
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
  FROM {{ source('natality', 'natality_data') }}, latest_year_data
  WHERE
    CAST(dob_yy AS INT64) = latest_year_data.latest_year  -- Proper reference
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
    ELSE 'Unknown'  -- Safety catch-all
  END AS age_group_description,
  birth_count,  -- Matches the renamed column
  percentage,
  (SELECT latest_year FROM latest_year_data) AS report_year  -- Added for context
FROM age_distribution
ORDER BY age_group