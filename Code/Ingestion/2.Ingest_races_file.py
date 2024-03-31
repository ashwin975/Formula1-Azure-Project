# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields = [StructField("raceid", IntegerType(),False),
                             StructField("year", IntegerType(), True),
                             StructField("round", IntegerType(), True),
                             StructField("circuitid", IntegerType(), True),
                             StructField("name", StringType(), True),
                             StructField("date", DateType(), True),
                             StructField("time", StringType(), True),
                             StructField("url", StringType(), True)
])

                    

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_select_df = races_df.select(col("raceid"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))

# COMMAND ----------

races_renamed_df = races_select_df.withColumnRenamed("raceid", "race_id") \
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("year", "race_year")

# COMMAND ----------

from pyspark.sql.functions import col, lit, to_timestamp, concat

# COMMAND ----------

races_final_df = races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_final_df = races_final_df.select(col("race_id"), col("race_year"), col("round"), col("circuit_id"), col("name"), col("race_timestamp"), col("data_source"))

# COMMAND ----------

races_final_df = add_ingestion_date(races_final_df)

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

#display(spark.read.parquet(f'{processed_folder_path}/races'))

# COMMAND ----------

dbutils.notebook.exit("Success")
