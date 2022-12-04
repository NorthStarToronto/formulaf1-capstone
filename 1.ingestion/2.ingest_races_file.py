# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook for cleansing and batch loading raw races CSV file into a managed Hive delta table 

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
# MAGIC ##### Step 1 - Read the CSV file with header using the spark dataframe reader

# COMMAND ----------

### Define the races schema using PySpark method
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

### Read data the into the Spark dataframe
races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

races_df.show(n = 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select and rename the required columns

# COMMAND ----------

from pyspark.sql.functions import col

races_renamed_df = races_df \
    .select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("date"), col("time")) 

races_renamed_df.show(n = 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Convert date and time columns into timestamp column

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, to_timestamp

races_with_timestamp_df = races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("file_date", lit(v_file_date))

races_with_timestamp_df.show(n = 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Add file date and ingestion date columns to the dataframe for auditing purpose

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

races_final_df = races_with_timestamp_df \
    .withColumn("file_date", lit(v_file_date)) \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 6 - Write the data to delta lake in full overwrite mode

# COMMAND ----------

races_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races LIMIT 3

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


