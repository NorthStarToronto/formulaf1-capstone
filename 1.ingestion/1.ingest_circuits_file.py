# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook for cleansing and batch loading raw circuits CSV file into a managed delta table 

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

### Define the circuits schema using PySpark method
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False),
                                       StructField("circuitRef", StringType(), True),
                                       StructField("name", StringType(), True),
                                       StructField("location", StringType(), True),
                                       StructField("country", StringType(), True),
                                       StructField("lat", DoubleType(), True),
                                       StructField("lng", DoubleType(), True),
                                       StructField("alt", IntegerType(), True),
                                       StructField("url", StringType(), True)
])

# COMMAND ----------

### Read the data into the Spark dataframe
circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

circuits_df.show(n = 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select and rename the required columns

# COMMAND ----------

from pyspark.sql.functions import col

circuits_renamed_df = circuits_df \
    .select(col("circuitId").alias("circuit_id"), col("circuitRef").alias("circuit_ref"), 
            col("name"), col("location"), col("country"), col("lat").alias("latitude"), 
            col("lng").alias("longitude"), col("alt").alias("altitude"))

circuits_df.show(n = 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Add file date and ingestion date columns to the dataframe for auditing purpose

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

circuits_final_df = circuits_renamed_df \
    .withColumn("file_date", lit(v_file_date)) \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the data to delta lake in full overwrite mode

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits LIMIT 3

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

circuits_df = spark.read \
    .option("inferSchema", True) \
    .option("header", True) \
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

circuits_df.dtypes

# COMMAND ----------

circuits_df.schema.fields

# COMMAND ----------

for e in circuits_df.schema.fields:
    str_list = str(e).split("'")
    str_list[1] = "'"+rename_field(str_list[1])+"'"
    print("".join(str_list))

# COMMAND ----------

def rename_field(field_name):
    import re
    temp_list = re.findall('^[a-z]+|[A-Z][^A-Z]*', field_name)
    temp_list = [word.lower() for word in temp_list]
    return ("_").join(temp_list)

# COMMAND ----------


