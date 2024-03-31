# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, DateType

# COMMAND ----------

pitstops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("stop", StringType(), True),
                                    StructField("lap", IntegerType(),True),
                                    StructField("time", StringType(),True),
                                    StructField("duration", StringType(),True),
                                    StructField("milliseconds", IntegerType(), True)

])

# COMMAND ----------

pitstops_df = spark.read \
    .schema(pitstops_schema) \
    .option("multiLine", True) \
    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

pitstops_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

pitstops_final_df = pitstops_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date)) 

# COMMAND ----------

pitstops_final_df = add_ingestion_date(pitstops_final_df)

# COMMAND ----------

display(pitstops_final_df)

# COMMAND ----------

#overwrite_partition(pitstops_final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

# pitstops_final_df.write.mode("append").format("parquet").saveAsTable("f1_processed.pitstops")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pitstops_final_df, 'f1_processed', 'pitstops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

#display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, Count(1)
# MAGIC FROM f1_processed.pitstops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, stop, COUNT(1)
# MAGIC FROM f1_processed.pitstops
# MAGIC GROUP BY race_id, driver_id, stop
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC
