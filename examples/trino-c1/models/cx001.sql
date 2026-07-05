/* Iowa Liquor Sales: parts de revenu mensuel par catégorie + évolution MoM (LAG) + anti-join catégories discontinuées */
WITH monthly_category_sales AS (
  SELECT
    DATE_TRUNC('MONTH', date) AS month,
    category_name,
    SUM(sale_dollars) AS revenue,
    SUM(bottles_sold) AS bottles_sold
  FROM iowa_liquor_sales.sales
  WHERE
    date BETWEEN '2021-01-01' AND '2022-12-31' AND NOT category_name IS NULL
  GROUP BY
    1,
    2
), categories_2020 /* Catégories présentes en 2020 */ AS (
  SELECT DISTINCT
    category_name
  FROM iowa_liquor_sales.sales
  WHERE
    EXTRACT(YEAR FROM date) = 2020 AND NOT category_name IS NULL
), categories_2021 /* Catégories présentes en 2021 */ AS (
  SELECT DISTINCT
    category_name
  FROM iowa_liquor_sales.sales
  WHERE
    EXTRACT(YEAR FROM date) = 2021 AND NOT category_name IS NULL
), discontinued_categories /* Anti-join : catégories présentes en 2020 mais absentes en 2021 (discontinuées) */ AS (
  SELECT
    c20.category_name
  FROM categories_2020 AS c20
  LEFT JOIN categories_2021 AS c21
    ON c20.category_name = c21.category_name
  WHERE
    c21.category_name IS NULL
), enriched AS (
  SELECT
    mcs.month,
    mcs.category_name,
    mcs.revenue,
    mcs.bottles_sold,
    ROUND(
      CAST(100.0 * mcs.revenue AS DOUBLE) / SUM(mcs.revenue) OVER (PARTITION BY mcs.month),
      2
    ) AS pct_monthly_revenue, /* Part du revenu mensuel total (window function partitionnée par mois) */
    LAG(mcs.revenue, 1) OVER (PARTITION BY mcs.category_name ORDER BY mcs.month NULLS FIRST) AS prev_month_revenue, /* Revenu du mois précédent */
    ROUND(
      CAST(100.0 * (
        mcs.revenue - LAG(mcs.revenue, 1) OVER (PARTITION BY mcs.category_name ORDER BY mcs.month NULLS FIRST)
      ) AS DOUBLE) / NULLIF(
        LAG(mcs.revenue, 1) OVER (PARTITION BY mcs.category_name ORDER BY mcs.month NULLS FIRST),
        0
      ),
      2
    ) AS mom_growth_pct, /* Croissance MoM en % */
    SUM(mcs.revenue) OVER (
      PARTITION BY mcs.category_name, EXTRACT(YEAR FROM mcs.month)
      ORDER BY mcs.month NULLS FIRST
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS ytd_revenue, /* Cumul YTD par catégorie (fenêtre de début d'année au mois courant) */
    RANK() OVER (PARTITION BY mcs.month ORDER BY mcs.revenue DESC) AS monthly_rank /* Classement mensuel par revenu décroissant */
  FROM monthly_category_sales AS mcs
  /* Anti-join : exclure les catégories discontinuées */
  LEFT JOIN discontinued_categories AS dc
    ON mcs.category_name = dc.category_name
  WHERE
    dc.category_name IS NULL
)
SELECT
  DATE_FORMAT(CAST(CAST(month AS TIMESTAMP) AS DATE), '%Y-%m') AS year_month,
  category_name,
  revenue,
  pct_monthly_revenue,
  prev_month_revenue,
  mom_growth_pct,
  ytd_revenue,
  monthly_rank
FROM enriched
WHERE
  monthly_rank <= 5
ORDER BY
  month NULLS FIRST,
  monthly_rank NULLS FIRST