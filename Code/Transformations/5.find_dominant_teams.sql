-- Databricks notebook source
SELECT * FROM f1_presentation.calaculated_race_results

-- COMMAND ----------

SELECT team, count(1) as total_races, 
SUM(calculated_points) as total_points, 
round(AVG(calculated_points),2) as avg_points
FROM f1_presentation.calaculated_race_results
WHERE race_year BETWEEN 2001 and 2011
GROUP BY team
HAVING count(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


