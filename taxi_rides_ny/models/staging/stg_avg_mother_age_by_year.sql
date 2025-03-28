{{
  config(
    materialized='view'
  )
}}

SELECT
  dob_yy AS year,
  ROUND(AVG(CAST(mager AS NUMERIC)), 2) AS avg_mother_age,
  COUNT(*) AS record_count,
  ROUND(STDDEV(CAST(mager AS NUMERIC)), 2) AS std_dev_age,
  MIN(CAST(mager AS NUMERIC)) AS min_age,
  MAX(CAST(mager AS NUMERIC)) AS max_age
FROM {{ source('natality', 'natality_data') }}
WHERE
  dob_date IS NOT NULL
  AND mager IS NOT NULL
  AND CAST(mager AS NUMERIC) BETWEEN 15 AND 50  -- Reasonable age range filter
GROUP BY year
ORDER BY year