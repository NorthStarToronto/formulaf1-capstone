# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook for incremental loading of joined delta tables in the processed layer into calculated race results reporting delta table in the presentation layer

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 0 - Create calculated race results tables using sql syntax

# COMMAND ----------

spark.sql(f"""
              CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
              (
              race_year INT,
              team_name STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              position INT,
              points INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
              )
              USING DELTA
""")

# COMMAND ----------

spark.sql(f"""DESC TABLE EXTENDED f1_presentation.calculated_race_results""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Create the temporary view as query from the joined processed tables with the normalized caculated points

# COMMAND ----------

spark.sql(f"""
              CREATE OR REPLACE TEMP VIEW race_result_updated
              AS
              SELECT races.race_year,
                     constructors.name AS team_name,
                     drivers.driver_id,
                     drivers.name AS driver_name,
                     races.race_id,
                     results.position,
                     results.points,
                     11 - results.position AS calculated_points
                FROM f1_processed.results 
                JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
                JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
                JOIN f1_processed.races ON (results.race_id = races.race_id)
               WHERE results.position <= 10
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Merge the temporay view result into the calculated race results delta table in the presentation database

# COMMAND ----------

spark.sql(f"""
              MERGE INTO f1_presentation.calculated_race_results tgt
              USING race_result_updated upd
              ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
              WHEN MATCHED THEN
                UPDATE SET tgt.position = upd.position,
                           tgt.points = upd.points,
                           tgt.calculated_points = upd.calculated_points,
                           tgt.updated_date = current_timestamp
              WHEN NOT MATCHED
                THEN INSERT (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, created_date ) 
                     VALUES (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, current_timestamp)
       """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM race_result_updated

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1) FROM f1_presentation.calculated_race_results

# COMMAND ----------

dbutils.notebook.exit("Success")
