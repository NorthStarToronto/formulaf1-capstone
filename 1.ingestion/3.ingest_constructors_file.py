# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook for cleansing and batch loading raw constructors JSON file into a managed Hive delta table 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 0 - Configure the file date parameter for modular loading of the raw data

# COMMAND ----------

# MAGIC %run "../0.utilities/0.configuration"

# COMMAND ----------

### Configure the file date parameter for loading the data by the file_date folder name
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../0.utilities/2.common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the single-line JSON file using the spark dataframe reader

# COMMAND ----------

### Define the constructors schema using PySpark method
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

constructors_schema = StructType(fields=[StructField("constructorId", IntegerType(), False),
                                  StructField("constructorRef", StringType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("nationality", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

### Read the data into the Spark dataframe
constructors_df = spark.read \
    .schema(constructors_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

constructors_df.show(n = 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select and rename the required columns

# COMMAND ----------

from pyspark.sql.functions import col

constructors_renamed_df = constructors_df \
    .select(col("constructorId").alias("constructor_id"), col("constructorRef").alias("constructor_ref"), 
            col("name"), col("nationality")) 

constructors_renamed_df.show(n = 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Add file date and ingestion date columns to the dataframe for auditing purpose

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

constructors_final_df = constructors_renamed_df \
    .withColumn("file_date", lit(v_file_date)) \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 6 - Write data to delta lake in full overwrite mode

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors LIMIT 3

# COMMAND ----------

dbutils.notebook.exit("Success")
