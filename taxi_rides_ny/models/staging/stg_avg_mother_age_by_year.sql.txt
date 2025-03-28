SELECT
  EXTRACT(YEAR FROM birth_date) AS year,
  ROUND(AVG(mother_age), 2) AS avg_mother_age,
  COUNT(*) AS record_count
FROM {{ ref('stg_birth_data') }}
WHERE EXTRACT(YEAR FROM birth_date) BETWEEN 2018 AND 2023
GROUP BY 1
ORDER BY year