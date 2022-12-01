# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook for cleansing and incremental loading raw lap times CSV files into a managed Hive delta table 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 0 - Configure the file date parameter for modular loading of the raw data

# COMMAND ----------

# MAGIC %run "../0.utilities/0.configuration"

# COMMAND ----------

### Configure the file date parameter for loading the data by the file_date folder name
dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../0.utilities/2.common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV files using the spark dataframe reader

# COMMAND ----------

### Define the lap times schema using PySpark method
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

### Read the data into the Spark dataframe
lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/lap_times")

lap_times_df.show(n = 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select and rename the required columns

# COMMAND ----------

from pyspark.sql.functions import col

lap_times_renamed_df = lap_times_df \
    .select(col("raceId").alias("race_id"), col("driverId").alias("driver_id"),
            col("lap"), col("position"), 
            col("time"), col("milliseconds")) 

lap_times_renamed_df.show(n = 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Add file date and ingestion date columns to the dataframe for auditing purpose

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

lap_times_final_df = lap_times_renamed_df \
    .withColumn("file_date", lit(v_file_date)) \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write data to delta lake in incremental overwrite mode

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"

merge_delta_data(lap_times_final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times LIMIT 3

# COMMAND ----------

dbutils.notebook.exit("Success")
