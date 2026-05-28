-- Iowa Liquor Sales: parts de revenu mensuel par catégorie + évolution MoM (LAG) + anti-join catégories discontinuées
WITH monthly_category_sales AS (
    SELECT
        DATE_TRUNC(date, MONTH)  AS month,
        category_name,
        SUM(sale_dollars)        AS revenue,
        SUM(bottles_sold)        AS bottles_sold
    FROM `bigquery-public-data.iowa_liquor_sales.sales`
    WHERE date BETWEEN '2021-01-01' AND '2022-12-31'
      AND category_name IS NOT NULL
    GROUP BY 1, 2
),

-- Catégories présentes en 2020
categories_2020 AS (
    SELECT DISTINCT category_name
    FROM `bigquery-public-data.iowa_liquor_sales.sales`
    WHERE EXTRACT(YEAR FROM date) = 2020
      AND category_name IS NOT NULL
),

-- Catégories présentes en 2021
categories_2021 AS (
    SELECT DISTINCT category_name
    FROM `bigquery-public-data.iowa_liquor_sales.sales`
    WHERE EXTRACT(YEAR FROM date) = 2021
      AND category_name IS NOT NULL
),

-- Anti-join : catégories présentes en 2020 mais absentes en 2021 (discontinuées)
discontinued_categories AS (
    SELECT c20.category_name
    FROM categories_2020 c20
    LEFT JOIN categories_2021 c21 ON c20.category_name = c21.category_name
    WHERE c21.category_name IS NULL
),

enriched AS (
    SELECT
        mcs.month,
        mcs.category_name,
        mcs.revenue,
        mcs.bottles_sold,

        -- Part du revenu mensuel total (window function partitionnée par mois)
        ROUND(
            100.0 * mcs.revenue / SUM(mcs.revenue) OVER (PARTITION BY mcs.month),
            2
        ) AS pct_monthly_revenue,

        -- Revenu du mois précédent
        LAG(mcs.revenue, 1) OVER (
            PARTITION BY mcs.category_name ORDER BY mcs.month
        ) AS prev_month_revenue,

        -- Croissance MoM en %
        ROUND(
            100.0
            * (mcs.revenue - LAG(mcs.revenue, 1) OVER (PARTITION BY mcs.category_name ORDER BY mcs.month))
            / NULLIF(LAG(mcs.revenue, 1) OVER (PARTITION BY mcs.category_name ORDER BY mcs.month), 0),
            2
        ) AS mom_growth_pct,

        -- Cumul YTD par catégorie (fenêtre de début d'année au mois courant)
        SUM(mcs.revenue) OVER (
            PARTITION BY mcs.category_name, EXTRACT(YEAR FROM mcs.month)
            ORDER BY mcs.month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS ytd_revenue,

        -- Classement mensuel par revenu décroissant
        RANK() OVER (PARTITION BY mcs.month ORDER BY mcs.revenue DESC) AS monthly_rank

    FROM monthly_category_sales mcs
    -- Anti-join : exclure les catégories discontinuées
    LEFT JOIN discontinued_categories dc ON mcs.category_name = dc.category_name
    WHERE dc.category_name IS NULL
)

SELECT
    FORMAT_DATE('%Y-%m', month) AS year_month,
    category_name,
    revenue,
    pct_monthly_revenue,
    prev_month_revenue,
    mom_growth_pct,
    ytd_revenue,
    monthly_rank
FROM enriched
WHERE monthly_rank <= 5
ORDER BY month, monthly_rank;
