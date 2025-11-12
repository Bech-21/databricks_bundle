CREATE OR REFRESH MATERIALIZED VIEW gold_earthquake_rig_summary
AS
WITH rig_by_state AS (
   SELECT 
      state_or_province,
      county,
        SUM(rig_count) AS total_rigs,
        AVG(rig_count) AS avg_daily_rigs,
        MAX(rig_count) AS peak_rigs
    FROM earth_data.lakehouse.silver_rig_count
    WHERE country ='UNITED STATES' and county IS NOT NULL
    GROUP BY state_or_province, county
),

earthquake_by_state AS (
  SELECT 
       county,
        COUNT(*) AS total_earthquakes,
        AVG(magnitude) AS avg_magnitude,
        MAX(magnitude) AS max_magnitude,
        COUNT_IF(magnitude>= 3.0) AS significant_events,
        AVG(sig) AS avg_significance
    FROM earth_data.lakehouse.silver_with_location
    WHERE county IS NOT NULL
    GROUP BY county
),

combined_data AS (
    SELECT 
       r.county,
        r.state_or_province,       
        COALESCE(r.total_rigs, 0) AS total_rigs,
        COALESCE(e.total_earthquakes, 0) AS total_earthquakes,
        COALESCE(e.avg_magnitude, 0) AS avg_magnitude
    FROM rig_by_state r
    INNER JOIN earthquake_by_state e
        ON r.county = e.county

)

SELECT 
*
FROM combined_data
ORDER BY total_rigs DESC