# Databricks notebook source
display(dbutils.fs.ls("abfss://demo@formula1storagedl1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1storagedl1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


