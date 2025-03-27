WITH base AS (
  SELECT 
    mager9 AS mother_age_group,
    COUNT(*) AS count
  FROM {{ ref('stg_birth_data') }}
  WHERE EXTRACT(YEAR FROM birth_date) = 2023
  GROUP BY 1
)

SELECT
  mother_age_group,
  count,
  ROUND(count * 100.0 / SUM(count) OVER(), 2) AS percentage
FROM base
ORDER BY mother_age_group