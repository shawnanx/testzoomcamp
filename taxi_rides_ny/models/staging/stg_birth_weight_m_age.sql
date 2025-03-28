{{
  config(
    materialized='view'
  )
}}

SELECT
  CAST(mager9 AS INT64) AS age_group,
  ROUND(AVG(CAST(dbwt AS NUMERIC)), 2) AS avg_birth_weight,
  COUNT(*) AS record_count,
  ROUND(STDDEV(CAST(dbwt AS NUMERIC)), 2) AS std_dev_birth_weight,
  MIN(CAST(dbwt AS NUMERIC)) AS min_birth_weight,
  MAX(CAST(dbwt AS NUMERIC)) AS max_birth_weight
FROM {{ source('natality', 'natality_data') }}
WHERE
  mager9 IS NOT NULL
  AND dbwt IS NOT NULL
  AND CAST(dbwt AS NUMERIC) <> 9999
  AND CAST(dbwt AS NUMERIC) > 0
GROUP BY mager9
ORDER BY age_group