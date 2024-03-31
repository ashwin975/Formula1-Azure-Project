# Databricks notebook source
def mount_adls(storage_account_name, container_name):
    client_secret = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1-serviceprincipal')
    client_id = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1-app-id')
    tenant_id = dbutils.secrets.get(scope= 'formula1-scope', key= 'formula1-tenant-id')

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs
        )
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("formula1storagedl1","raw")

# COMMAND ----------

mount_adls("formula1storagedl1","processed")

# COMMAND ----------

mount_adls("formula1storagedl1","presentation")

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

display(dbutils.fs.ls("/mnt/formula1storagedl1/demo"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1storagedl1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dl1/demo")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


