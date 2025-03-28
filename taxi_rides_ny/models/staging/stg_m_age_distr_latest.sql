WITH latest_year AS (
  SELECT 
    MAX(EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', dob_date)) AS latest_year
  FROM {{ source('natality', 'natality_data') }}
),

age_distribution AS (
  SELECT
    CAST(mager9 AS INT64) AS age_group,
    COUNT(*) AS birth_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
  FROM {{ source('natality', 'natality_data') }}
  WHERE
    EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', dob_date)) = (SELECT latest_year FROM latest_year)
    AND mager9 IS NOT NULL
    AND CAST(mager9 AS INT64) BETWEEN 1 AND 9  -- Valid age group codes
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
  END AS age_group_description,
  birth_count,
  percentage
FROM age_distribution
ORDER BY age_group