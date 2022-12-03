# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook for cleansing and incremental loading raw results JSON file into a managed Hive delta table 

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
# MAGIC ##### Step 1 - Read the multi-line JSON file using the spark dataframe reader

# COMMAND ----------

### Define the pit stops schema using PySpark method
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

### Read the data into the Spark dataframe
pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .option("multiLine", True) \
    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

pit_stops_df.show(n = 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select and rename the required columns

# COMMAND ----------

from pyspark.sql.functions import col

pit_stops_renamed_df = pit_stops_df \
    .select(col("raceId").alias("race_id"), col("driverId").alias("driver_id"),
            col("stop"), col("lap"), 
            col("time"), col("milliseconds")) 

pit_stops_renamed_df.show(n = 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Add file date and ingestion date columns to the dataframe for auditing purpose

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

pit_stops_final_df = pit_stops_renamed_df \
    .withColumn("file_date", lit(v_file_date)) \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the data to delta lake in incremental overwrite mode

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"

merge_delta_data(pit_stops_final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops LIMIT 3

# COMMAND ----------

dbutils.notebook.exit("Success")
