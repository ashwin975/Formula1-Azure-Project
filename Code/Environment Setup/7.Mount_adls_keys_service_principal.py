# Databricks notebook source
client_secret = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1-serviceprincipal')
client_id = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1-app-id')
tenant_id = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1-tenant-id')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1storagedl1.dfs.core.windows.net/",
  mount_point = "/mnt/formula1storagedl1/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1storagedl1/raw"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1storagedl1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dl1/demo")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dl1storagedl1/")
