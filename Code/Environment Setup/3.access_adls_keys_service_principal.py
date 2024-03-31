# Databricks notebook source
formula1dl_serviceprincipal = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1-serviceprincipal')

# COMMAND ----------

client_id = "7e3e6a64-e971-42f8-b447-d9c39b977035"
tenant_id = "ef43eb23-a31f-4fdb-a5a5-44166d5525a3"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1storagedl1.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1storagedl1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1storagedl1.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1storagedl1.dfs.core.windows.net", formula1dl_serviceprincipal)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1storagedl1.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1storagedl1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1storagedl1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


