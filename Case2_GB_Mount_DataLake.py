# Databricks notebook source
dbutils.fs.mkdirs("/mnt/gb/email")

# COMMAND ----------

dbutils.fs.ls("/mnt/gb/email")

# COMMAND ----------

client_id = "7d2bfde3-b52c-426a-80a0-7feab0982ddc"
client_secret = "muQ8Q~~Pl0rAYSUQKJroZyWiAc6pepdcWWms1dxa"
client_endpoint = "https://login.microsoftonline.com/12de17f8-7229-421f-8ee3-b9387494e0c3/oauth2/token"
container = "ctdadosgbcase2email"
storage_acount = "stgbcas2"
mount_path_point = "/mnt/gb/email"

# COMMAND ----------

configs = {f"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": client_id,
            "fs.azure.account.oauth2.client.secret": client_secret,
            "fs.azure.account.oauth2.client.endpoint": client_endpoint,
            "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount( source = f"abfss://{container}@{storage_acount}.dfs.core.windows.net/",
                  mount_point = mount_path_point,
                  extra_configs = configs)

# COMMAND ----------

#dbutils.fs.unmount("/mnt/gb/email") 

# COMMAND ----------

dbutils.fs.ls("/mnt/gb/email")
