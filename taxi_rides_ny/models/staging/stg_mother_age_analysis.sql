create or replace view `natality-data-project-dez`.`natality_data`.`maternal_age_analysis`
OPTIONS()
as 

SELECT
  mager9 AS age_group,
  AVG(dbwt) AS avg_birth_weight,
  COUNT(*) AS record_count,
  STDDEV(dbwt) AS std_dev_birth_weight,
  MIN(dbwt) AS min_birth_weight,
  MAX(dbwt) AS max_birth_weight
FROM
  `natality-data-project-dez.natality_data.natality_data`
WHERE
  mager9 IS NOT NULL
  AND dbwt IS NOT NULL
  AND dbwt <> 9999
  AND dbwt > 0
GROUP BY
  mager9
ORDER BY
  mager9;

