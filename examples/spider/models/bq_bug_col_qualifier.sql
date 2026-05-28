-- Regression test: full backtick-quoted table path used as column qualifier.
-- Pattern: `project.dataset.table`.column  (au lieu de table.column ou alias.column)
-- Ce pattern déclenchait "unrecognized name" lors du dry-run BigQuery
-- et une erreur DuckDB lors de l'exécution locale.
SELECT
  `bigquery-public-data.the_met.objects`.object_id,
  `bigquery-public-data.the_met.objects`.title,
  `bigquery-public-data.the_met.objects`.artist_display_name,
  `bigquery-public-data.the_met.objects`.medium
FROM
  `bigquery-public-data.the_met.objects`
WHERE
  `bigquery-public-data.the_met.objects`.department = 'Photographs'
  AND `bigquery-public-data.the_met.objects`.object_end_date <= 1900
