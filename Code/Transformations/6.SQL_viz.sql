-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Drivers </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       ROUND(AVG(calculated_points),2) as avg_points,
       RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
FROM f1_presentation.calaculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC 

-- COMMAND ----------

SELECT race_year, 
      driver_name,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       ROUND(AVG(calculated_points),2) as avg_points
FROM f1_presentation.calaculated_race_results
WHERE driver_name in (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC 

-- COMMAND ----------

SELECT race_year, 
      driver_name,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       ROUND(AVG(calculated_points),2) as avg_points
FROM f1_presentation.calaculated_race_results
WHERE driver_name in (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC 

-- COMMAND ----------

SELECT race_year, 
      driver_name,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       ROUND(AVG(calculated_points),2) as avg_points
FROM f1_presentation.calaculated_race_results
WHERE driver_name in (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <= 10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team, count(1) as total_races, 
SUM(calculated_points) as total_points, 
round(AVG(calculated_points),2) as avg_points,
RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
FROM f1_presentation.calaculated_race_results
GROUP BY team
HAVING count(1) >= 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT * FROM v_dominant_teams

-- COMMAND ----------

SELECT race_year, team, 
count(1) as total_races, 
SUM(calculated_points) as total_points, 
round(AVG(calculated_points),2) as avg_points
FROM f1_presentation.calaculated_race_results
WHERE team in (SELECT team FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT race_year, team, 
count(1) as total_races, 
SUM(calculated_points) as total_points, 
round(AVG(calculated_points),2) as avg_points
FROM f1_presentation.calaculated_race_results
WHERE team in (SELECT team FROM v_dominant_teams WHERE team_rank <= 5)
GROUP BY race_year, team
ORDER BY avg_points DESC

-- COMMAND ----------


