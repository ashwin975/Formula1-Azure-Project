# 🚀 Formula1-Azure-Project

## Introduction:

The project is on building a cloud data pipeline for reporting and analysis of Formula1 Motor sports data.

This project leveraged the power of Azure Data Lake Gen2 for Datalake, Azure Databricks for processing the transformation, and Azure Key Vault for securely accessing the data from Datalake within the Notebook.

## 🔑 Learnings and achievements.
- ✅ Build an Architecture diagram for data flow ( raw -> processed -> presentation)
- ✅ Build Data Lake using Azure Data Lake Gen 2
- ✅ Securely stored the secrets/credentials using Azure Key Vault
- ✅ Transformed the data using Azure Databricks for reporting and analysis
- ✅ Analyzed the data using Databricks and created Dashboard
- ✅ Good understanding of the data to implement a business use case
- ✅ Created External and Managed tables using Spark (PySpark and Spark SQL)

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

###Ingestion Layer: (Raw ingestion)

Data stored in the ADLS gen 2, was ingested and processed by spark clusters - databricks

- Why Databricks? - Parallelized workload using Spark - Java Virtual Machines

- Files were ingested using spark dataframe reader, columns were renamed for data readability, with appropriate DDL based schema specified and unnecessary columns dropped
Alternative - we could also infer schema, suitable for dev or test environments

###Transformation Layer: (Filtered, cleaned, augmented information: silver)

The ingested files were cleaned and transformed as per needs and written in Parquet format to the processed container. Some of the large files were partitioned, so that spark could read quickly and process faster.
(eg. races file partitioned by races.year). The Distribution of the columns were also checked to avoid Data skews.

- Why Parquet?

Common notebooks were created for function calling, mounted folder paths and passing parameters as widgets from child notebooks using "%run" command
Notebook workflow was setup for testing ingestion and transformation notebooks within databricks and ran it using JOB cluster

###Analyze Layer: (Highly aggregated information: Gold):

Data from 5 different tables in the processed layer were utilized. Performed Filter Transformations, Multiple Joins, Aggregations, Temp views(accessed spark dataframes using SQL)
Local Temp views - accessible only within the notebook, Global Temp View - Accessible from more than one notebook
Metastore is created to build a relational abstract layer between spark and adls and use managed tables (CTAS statements) on top of it.

Why Managed Tables? Spark managed tables can delete both files and the metadata from the notebook itself. In External tables only the relational metadata can be deleted

Key Benefits of Using Unity Catalog: 
1. Data Discoverability: Data explorer provides a simple search through for any objects in catalog (with UI and SQL queries)
2. Data Audit: Audit logs/information can be viewed (when diagnostic settings are enabled)
3. Data Lineage: workflow, downstream and upstream datamovements can be viewed. With this we can perform root cause analysis, impact analysis and better manage data requiring regulatory compliance  
4. Data Access Control: Metastore access can be modified as required or as per user level (GRANT and REVOKE statements - SQL, CLI, also using Data Explorer)

## 🛠️ Tools Used:
 - Programming Language - SQL, PySpark, Python
 - Azure Cloud, Databricks, ADF
 - Azure Data lake Gen 2 Storage

## 🍽️ Dataset Used:
ERGAST API - Formula 1 Dataset - All races since 1950 : 2021

## Learning Resources:
Microsoft Architecture Frameworks - https://learn.microsoft.com/en-us/azure/architecture/browse/
PySpark Documentation - https://spark.apache.org/docs/latest/api/python/getting_started/index.html
