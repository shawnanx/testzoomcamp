SELECT
  mother_age_group,
  ROUND(AVG(birth_weight_grams), 2) AS avg_baby_weight_grams,
  COUNT(*) AS record_count
FROM {{ ref('stg_birth_data') }}
GROUP BY 1
ORDER BY mother_age_group