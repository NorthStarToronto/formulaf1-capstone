-- Databricks notebook source
-- MAGIC %run "../0.utilities/0.configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"CREATE DATABASE IF NOT EXISTS f1_presentation LOCATION '{presentation_folder_path}'")

-- COMMAND ----------

DESC DATABASE f1_presentation
