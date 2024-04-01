# 🚀 Formula1-Azure-Project

## Introduction:

The project is on building a cloud data pipeline for reporting and analysis of Formula1 Motor sports data.

This project leveraged the power of Azure Data Lake Gen2 for Datalake, Azure Databricks for processing the transformation, and Azure Key Vault for securely accessing the data from Datalake within the Notebook.

## 🔑 Learnings and achievements.
- ✅ Build Efficient data flow layers ( raw -> processed -> presentation)
- ✅ Monitoring and Managing Pipelines with Azure Monitor service
- ✅ Setup up (CI/CD) pipelines to facilitate testing, deployment, and production phases
- ✅ Build Delta Lake in Azure databricks for ACID transactions
- ✅ Created External and Managed tables using Spark (PySpark and Spark SQL)
- ✅ Securely stored the secrets/credentials using Azure Key Vault
- ✅ Transformed the data using Azure Databricks for reporting and analysis
- ✅ Analyzed the data using Databricks and created Dashboards using Power BI

## 🏛️ Architecture:

![alt text](https://github.com/ashwin975/Formula1-Azure-Project/blob/main/Formula1-Azure%20(1).svg)

## 𝍖 Data Model:

![alt text](https://github.com/ashwin975/Formula1-Azure-Project/blob/main/formula1_ergast_db_data_model.png)

## 🧅 Process Overview:

Initial Azure resources - resource groups, storage containers, databricks workspace, adf, key-vault service, linked services, databricks access connectors were setup

Datalake access from databricks was estabilshed using Service principal (formua1-app: client_id, tenant_id, secret_value), assigned with storage blob data contributor role to access datalake
Alternative - Azure Active Directory Passthrough for multiple users needing access to different storage accounts (should enable AAD passthrough at the cluster config)

- Why use Azure Key vault Secret Scope? - One place for all secrets, secrets can be shared among other azure services, RBAC can be provisioned as well
  
then created a function for mounting different storage accounts and containers, so that raw files could be mounted ---> databricks files system

- Why Mounting? Access data from blob storage without requiring credentials after authentication using Service Principal

Loading was carried out with both full load and incremental load scenarios (hybrid setup)

Day 1 Cutover file : contains historic data until ID 1047

Day 2: Contains weekly data, ID 1052

Day 3: Contains weekly data, ID 1053

Incremental data is for only results, pitstops, qualifying and Laptimes other files have full loads

Incremental load was done using INSERT INTO method run on last column with dynamic pratition by mode (to overwriting the entire table)

### Ingestion Layer: (Raw ingestion: Bronze)

Data stored in the ADLS gen 2, was ingested and processed by spark clusters - databricks

- Why Databricks? - Parallelized workload using Spark - Java Virtual Machines

- Files were ingested using spark dataframe reader, columns were renamed for data readability, with appropriate DDL based schema specified and unnecessary columns dropped
Alternative - we could also infer schema, suitable for dev or test environments

### Transformation Layer: (Filtered, cleaned, augmented information: Silver)

The ingested files were cleaned and transformed as per needs and written in Parquet format to the processed container. Some of the large files were partitioned, so that spark could read quickly and process faster.
(eg. races file partitioned by races.year). The Distribution of the columns were also checked to avoid Data skews.

- Why Parquet?

Common notebooks were created for function calling, mounted folder paths and passing parameters as widgets from child notebooks using "%run" command

Notebook workflow was setup for testing ingestion and transformation notebooks within databricks and ran it using JOB cluster

### Analyze Layer: (Highly aggregated information: Gold):

Data from 5 different tables in the processed layer were utilized. Performed Filter Transformations, Multiple Joins, Aggregations, Temp views(accessed spark dataframes using SQL)

Local Temp views - accessible only within the notebook, Global Temp View - Accessible from more than one notebook (cluster attached). Permanent view attached to hive metastore. (Views don't store data unlike tables)

Metastore is created to build a relational abstract layer between spark and adls and use managed tables (CTAS statements) on top of it.

Created Database for Raw, Processed, Presentation for tables storage

External tables for Raw layer as api data is stored, Managed tables for Processed and Presentation layer

Why Managed Tables? Spark managed tables can delete both files and the metadata from the notebook itself. In External tables only the relational metadata can be deleted

Built - race_results tables (resuts joined using races, drivers and constructors tables)

Created a separate points calculation metric with first position as 10 points...10th as 1 point then 0s. This clarified points given to drivers since 1950s

Presentation layer consists - dominant drivers table since 1950 (name, avg_points, total_races), dominant teams since 1950

Finally, based on the tables and views created in the presentation layer, report with simple visulaizations were built within the databricks workspace.

![alt text](https://github.com/ashwin975/Formula1-Azure-Project/blob/main/Visuals/chart%20(1).png)

![alt text](https://github.com/ashwin975/Formula1-Azure-Project/blob/main/Visuals/chart%20(2).png)

![alt text](https://github.com/ashwin975/Formula1-Azure-Project/blob/main/Visuals/chart.png)

Azure Data Factory was setup for ingestion files in databricks workspace under  pl_ingestion pipeline, scheduled with an event trigger

Another pipeline pl_transformation for transformation files. Lastly, a third pipeline pl_processing to execute the former two pipelines. So that only on successful completion of pipeline 1, the 2nd runs.

In the End after successful setup and completion of pipeline runs, Power BI was connected to the azure databricks workspace for enhanced BI reporting.

## 🛠️ Tools Used:
 - Programming Language - SQL, PySpark, Python
 - Azure Cloud, Databricks, ADF
 - Azure Data lake Gen 2 Storage

## 🍽️ Dataset Used:
ERGAST API - Formula 1 Dataset - All races since 1950 : 2021

## Learning Resources:
Microsoft Architecture Frameworks - https://learn.microsoft.com/en-us/azure/architecture/browse/
PySpark Documentation - https://spark.apache.org/docs/latest/api/python/getting_started/index.html
