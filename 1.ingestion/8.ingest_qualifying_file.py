# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook for cleansing and incremental loading raw qualifying JSON files into a managed Hive delta table 

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
# MAGIC ##### Step 1 - Read the JSON files using the spark dataframe reader

# COMMAND ----------

### Define the qualifying schema using PySpark method
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

### Read the data into the Spark dataframe
qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option("multiLine", True) \
    .json(f"{raw_folder_path}/{v_file_date}/qualifying")

qualifying_df.show(n = 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select and rename the required columns

# COMMAND ----------

from pyspark.sql.functions import col

qualifying_renamed_df = qualifying_df \
    .select(col("qualifyId").alias("qualify_id"), col("raceId").alias("race_id"), 
            col("driverId").alias("driver_id"), col("constructorId").alias("constructor_id"),
            col("number"), col("position"), 
            col("q1"), col("q2"), col("q3")) 

qualifying_renamed_df.show(n = 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Add file date and ingestion date columns to the dataframe for auditing purpose

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

qualifying_final_df = qualifying_renamed_df \
    .withColumn("file_date", lit(v_file_date)) \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the data to delta lake in incremental overwrite mode

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"

merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying LIMIT 3

# COMMAND ----------

dbutils.notebook.exit("Success")
