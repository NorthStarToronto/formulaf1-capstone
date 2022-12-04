# Overview
The goal of the data engineering capstone project is to create data pipeline and orchestration workflow templates for generating the key optimization and risk-management measurements and metrics for reporting, dashboard, and machine learning purposes. The data engineering workflow focuses on storing, organizing, and processing high volumes of low-density, structured and unstructured data in diverse file formats in a timely, effective, and efficient manner.


# Solution Architecture - Infrastructure
### Azure Delta Lake -  Data Storage Solution (highly available, scalable, managed, and distributed object-file system on public cloud).
* Transform and query data files in a distributed manner through Spark engine utilizing Delta Lake's Hive metastore. 
* Optimize storage cost through lifecycle management policy and  columnar file format.

### Azure Databricks Workspace and Spark Cluster - Data Processing Solution (scalable and fully managed Apache Spark environment and cluster on public cloud).
* Dedicate more time in building and managing data engineering pipelines instead of upgrading and maintaining the spark cluster. 
* Maximize large-scale unified data processing engine capability of Spark cluster through Azure Databricks workspace. 
* Promote fast, easy, secure, and scalable collaboration on the shared workspace with cluster configuration, git integration, and Active Directory configuration.

### Azure Data Factory - Data Orchestration Solution (scalable, intuitive, and serverless data orchestration tool on public cloud).
* Automate end-to-end orchestration at a regular interval or when an event occurs.
* Incorporate testing and dependency management.
* Enable easy troubleshooting of issues and monitoring of the entire orchestration progress.


# Workflows
### Databricks Notebooks
* Define the key optimization and risk-management measurements and metrics based on the business environment, constraint variables, and driver variables.
* Review the related fact table data, dimension table data, and their relationships.
* Review the data loading and transforming patterns. 
* Review the data model.
* Import the raw data to Azure delta lake's raw container. 
* Clean and transform the raw container data and store them as delta lake table in the process container.
* Join the required fact and dimension tables in the process container and generate reporting, dashboard, and machine learning delta tables.

### ADF Pipeline


Rerferences: Azure Databricks & Spark Core For Data Engineers(Python/SQL) and Azure Data Factory For Data Engineers - Project on Covid19 by Ramesh Retnasamy on Udemy
