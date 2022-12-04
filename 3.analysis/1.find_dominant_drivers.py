# Databricks notebook source
dbutils.widgets.text("p_beg_year", "")
v_beg_year = dbutils.widgets.get("p_beg_year")

# COMMAND ----------

dbutils.widgets.text("p_end_year", "")
v_end_year = dbutils.widgets.get("p_end_year")

# COMMAND ----------

spark.sql(f"""
                SELECT driver_name,
                       COUNT(1) AS total_races,
                       SUM(calculated_points) AS total_points,
                       AVG(calculated_points) AS avg_points
                FROM f1_presentation.calculated_race_results
                WHERE race_year BETWEEN '{v_beg_year}' AND '{v_end_year}'
                GROUP BY driver_name
                HAVING COUNT(1) >= 50
                ORDER BY avg_points DESC
""").show()
