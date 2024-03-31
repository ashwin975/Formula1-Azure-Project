-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION '/mnt/formula1storagedl1/processed';

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION '/mnt/formula1storagedl1/presentation';

-- COMMAND ----------

-- Drop the individual tables within the database
DROP TABLE IF EXISTS f1_processed.pitstops

-- COMMAND ----------



-- COMMAND ----------


