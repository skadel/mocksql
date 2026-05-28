-- Austin Bikeshare: performance mensuelle par station + % du district + LAG MoM + anti-join stations sans historique
WITH monthly_trips AS (
    SELECT
        DATE_TRUNC(start_time, MONTH) AS month,
        start_station_id,
        COUNT(*)                       AS trip_count,
        ROUND(AVG(duration_minutes), 1) AS avg_duration_min,
        COUNTIF(subscriber_type = 'Subscriber') AS subscriber_trips
    FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
    WHERE start_time BETWEEN '2018-01-01' AND '2019-12-31'
      AND start_station_id IS NOT NULL
    GROUP BY 1, 2
),

station_info AS (
    SELECT
        station_id,
        name            AS station_name,
        council_district AS district
    FROM `bigquery-public-data.austin_bikeshare.bikeshare_stations`
),

-- Stations actives en 2018 (historique de référence)
stations_2018 AS (
    SELECT DISTINCT start_station_id
    FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
    WHERE EXTRACT(YEAR FROM start_time) = 2018
),

-- Stations actives en 2019
stations_2019 AS (
    SELECT DISTINCT start_station_id
    FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
    WHERE EXTRACT(YEAR FROM start_time) = 2019
),

-- Anti-join : stations présentes en 2019 mais absentes en 2018 (ouverture récente, pas de historique MoM complet)
newly_opened AS (
    SELECT s19.start_station_id
    FROM stations_2019 s19
    LEFT JOIN stations_2018 s18 ON s19.start_station_id = s18.start_station_id
    WHERE s18.start_station_id IS NULL
),

enriched AS (
    SELECT
        mt.month,
        mt.start_station_id,
        si.station_name,
        si.district,
        mt.trip_count,
        mt.avg_duration_min,
        mt.subscriber_trips,

        -- % des trajets du district ce mois-ci (window function partitionnée par mois + district)
        ROUND(
            100.0 * mt.trip_count / SUM(mt.trip_count) OVER (PARTITION BY mt.month, si.district),
            2
        ) AS pct_district_trips,

        -- Trajets le mois précédent (pour la même station)
        LAG(mt.trip_count, 1) OVER (
            PARTITION BY mt.start_station_id ORDER BY mt.month
        ) AS prev_month_trips,

        -- Écart absolu MoM
        mt.trip_count
        - LAG(mt.trip_count, 1) OVER (PARTITION BY mt.start_station_id ORDER BY mt.month)
        AS mom_trips_delta,

        -- Évolution MoM en % par rapport au mois précédent
        ROUND(
            100.0
            * (mt.trip_count - LAG(mt.trip_count, 1) OVER (PARTITION BY mt.start_station_id ORDER BY mt.month))
            / NULLIF(LAG(mt.trip_count, 1) OVER (PARTITION BY mt.start_station_id ORDER BY mt.month), 0),
            2
        ) AS mom_growth_pct,

        -- Moyenne glissante sur 3 mois (lisse la saisonnalité)
        ROUND(
            AVG(mt.trip_count) OVER (
                PARTITION BY mt.start_station_id
                ORDER BY mt.month
                ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
            ),
            1
        ) AS rolling_3m_avg,

        -- Classement par nombre de trajets au sein du district ce mois-ci
        RANK() OVER (PARTITION BY mt.month, si.district ORDER BY mt.trip_count DESC) AS district_rank

    FROM monthly_trips mt
    INNER JOIN station_info si
        ON SAFE_CAST(mt.start_station_id AS INT64) = si.station_id
    -- Anti-join : exclure les stations ouvertes uniquement en 2019 (historique MoM incomplet)
    LEFT JOIN newly_opened no ON mt.start_station_id = no.start_station_id
    WHERE no.start_station_id IS NULL
)

SELECT
    FORMAT_DATE('%Y-%m', month) AS year_month,
    start_station_id,
    station_name,
    district,
    trip_count,
    pct_district_trips,
    prev_month_trips,
    mom_trips_delta,
    mom_growth_pct,
    rolling_3m_avg,
    district_rank
FROM enriched
WHERE district_rank <= 3
ORDER BY month, district, district_rank;
