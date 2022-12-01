-- Databricks notebook source
-- MAGIC %run "../0.utilities/0.configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"CREATE DATABASE IF NOT EXISTS f1_processed LOCATION '{processed_folder_path}'")

-- COMMAND ----------

DESC DATABASE f1_processed
