-- Databricks notebook source
Create DATABASE f1_raw

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
Create Table IF not exists f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING, 
  Country STRING,
  lat DOUBLE,
  lng Double,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path "/mnt/formula1storagedl1/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
Create Table IF not exists f1_raw.races(
  raceId INT,
  year STRING,
  round INT,
  circuitid INT,
  name STRING, 
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "/mnt/formula1storagedl1/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
Create Table IF not exists f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "/mnt/formula1storagedl1/raw/constructors.json", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
Create Table IF not exists f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS (path "/mnt/formula1storagedl1/raw/drivers.json", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
Create Table IF not exists f1_raw.results(
  resultId INT,
  raceId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  position_text STRING,
  positionOrder INT,
  laps INT,
  milliseconds INT,
  points INT,
  fastestLap INT,
  fastestLapSpeed FLOAT,
  fastestLapTime STRING,
  statusId INT,
  rank INT,
  time STRING
)
USING json
OPTIONS (path "/mnt/formula1storagedl1/raw/results.json", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
Create Table IF not exists f1_raw.pit_stops(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
USING json
OPTIONS (path "/mnt/formula1storagedl1/raw/pit_stops.json", multiline true)

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
Create Table IF not exists f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING csv
OPTIONS (path "/mnt/formula1storagedl1/raw/lap_times")

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
Create Table IF not exists f1_raw.qualifying(
  qualifyId INT,
  driverId INT,
  raceId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING json
OPTIONS (path "/mnt/formula1storagedl1/raw/qualifying", multiline true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying

-- COMMAND ----------

DESC EXTENDED f1_raw.qualifying
