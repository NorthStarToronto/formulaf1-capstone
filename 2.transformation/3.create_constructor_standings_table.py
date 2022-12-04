# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook for incremental loading and transforming of race results reporting delta table in the presentation layer into constructor standings dashboard delta table in the presentation layer

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 0 - Import the configuration settings and common functions

# COMMAND ----------

# MAGIC %run "../0.utilities/0.configuration"

# COMMAND ----------

# MAGIC %run "../0.utilities/2.common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the required reporting data

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \

race_results_df.show(n = 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Implement the required business logic using the aggregate and window transformations

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standings_df = race_results_df \
    .groupBy("race_year", "driver_name", "driver_nationality") \
    .agg(sum("points").alias("total_points"),
         count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write the data to delta lake in incremental overwrite mode

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"

merge_delta_data(final_df, 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.driver_standings WHERE race_year = 2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1)
# MAGIC   FROM f1_presentation.driver_standings
# MAGIC  GROUP BY race_year
# MAGIC  ORDER BY race_year DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")
