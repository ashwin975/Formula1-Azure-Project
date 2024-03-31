# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %fs
# MAGIC ls '/mnt/formula1storagedl1/processed'

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df = circuits_df.withColumnRenamed("name", "circuit_name") \
.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races")

# COMMAND ----------

races_df = races_df.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date") 

# COMMAND ----------

display(races_df)

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers")

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors")

# COMMAND ----------

constructors_df = constructors_df.withColumnRenamed("name", "team") \
.withColumnRenamed("nationality", "constructors_nationality")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results")

# COMMAND ----------

results_df = results_df \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time", "race_time") \
.withColumnRenamed("race_id", "result_race_id") \
.withColumnRenamed("file_date", "result_file_date") 

# COMMAND ----------

display(results_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner").select(circuits_df.circuit_location, races_df.race_id, races_df.race_name, races_df.race_year, races_df.race_date)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id) \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id) \
    .select("race_id", "race_year", "race_name", "race_date", "circuit_location",
            "driver_name", "driver_number", "driver_nationality",
            "team", "grid", "fastest_lap", "race_time", "points", "position",
            "result_file_date") \
    .withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_df = created_date(race_results_df)

# COMMAND ----------

display(race_results_df.filter("race_year = 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(race_results_df.points.desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC race_results_df.write.mode('overwrite').partitionBy("race_year").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

#overwrite_partition(race_results_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(race_results_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

display(spark.read.format("delta").load(f"{presentation_folder_path}/race_results"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, Count(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC
