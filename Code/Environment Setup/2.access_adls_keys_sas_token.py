# Databricks notebook source
formula1dl_sas = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1-sas')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1storagedl1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1storagedl1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1storagedl1.dfs.core.windows.net", formula1dl_sas)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1storagedl1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1storagedl1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


