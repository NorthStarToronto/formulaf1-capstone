# Databricks notebook source
storage_account_name = "formulaf1jhdl"
databricks_secrets_scope = "formulaf1-scope"
client_id = dbutils.secrets.get(databricks_secrets_scope, "databricks-app-client-id")
tenant_id = dbutils.secrets.get(databricks_secrets_scope, "databricks-app-tenant-id")
client_secret = dbutils.secrets.get(databricks_secrets_scope, "databricks-app-client-secret")

# COMMAND ----------

### Setup Config Parameters
configs =  {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": f"{client_id}",
            "fs.azure.account.oauth2.client.secret": f"{client_secret}",
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
            "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

# COMMAND ----------

def mount_adls(container_name):
    """
        Mount Azure Delta Lake continer to Databrick cluster's dbfs root mnt directory
        :params container_name: container name
        returns:
            True if mount is succesful
    """
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)

# COMMAND ----------

### Create the mount point for raw container
mount_adls("raw")

# COMMAND ----------

### Validate mounting the raw container
dbutils.fs.ls(f"/mnt/{storage_account_name}/raw")

# COMMAND ----------

### Create the mount point for processed container
mount_adls("processed")

# COMMAND ----------

### Validate mounting the raw container
dbutils.fs.ls(f"/mnt/{storage_account_name}/processed")

# COMMAND ----------

### Create the mount point for presentation container
mount_adls("presentation")

# COMMAND ----------

### Validate mounting the raw container
dbutils.fs.ls(f"/mnt/{storage_account_name}/presentation")

# COMMAND ----------


