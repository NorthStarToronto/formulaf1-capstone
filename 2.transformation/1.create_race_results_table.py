# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook for incremental loading of joined delta tables in the processed layer into race results reporting delta table in the presentation layer

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 0 - Import the configuration settings and common functions

# COMMAND ----------

# MAGIC %run "../0.utilities/0.configuration"

# COMMAND ----------

# MAGIC %run "../0.utilities/2.common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read all the required processed data and renamed the required columns

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("nationality", "driver_nationality")

drivers_df.show(n = 3)

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team") 

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location", "circuit_location") 

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date") 

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "result_race_id") \
    .withColumnRenamed("file_date", "result_file_date") 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Join all the required dataframes to create race results dataframe

# COMMAND ----------

### Join races_df with circuits_df 
race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id) \
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

### Join results_df with race_circuits_df 
### Join results_df with drivers_df
### Join results_df with constructors_df
race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select the required columns and add created date & file date columns to the dataframe for auditing purpose

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position", "result_file_date") \
                          .withColumn("created_date", current_timestamp()) \
                          .withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the data to delta lake in incremental overwrite mode

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"

merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_presentation.race_results LIMIT 3

# COMMAND ----------

dbutils.notebook.exit("Success")
