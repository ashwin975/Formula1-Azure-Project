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

## Architecture:

![alt text](https://github.com/ashwin975/Formula1-Azure-Project/blob/main/Formula1-Azure%20(1).svg)

## Process Overview:
- Storage account, Databricks workspace, Databricks access connector, Resource group, Databricks metastore, cluster configurations were setup initially
- Connections and access management between Azure storage container and databricks workspace were established
- Ingestion and Transformations notebooks were created using SQL
- Workflow was orchestrated to run the notebooks sequentially

Key Benefits of Using Unity Catalog: 
1. Data Discoverability: Data explorer provides a simple search through for any objects in catalog (with UI and SQL queries)
2. Data Audit: Audit logs/information can be viewed (when diagnostic settings are enabled)
3. Data Lineage: workflow, downstream and upstream datamovements can be viewed. With this we can perform root cause analysis, impact analysis and better manage data requiring regulatory compliance  
4. Data Access Control: Metastore access can be modified as required or as per user level (GRANT and REVOKE statements - SQL, CLI, also using Data Explorer)

## Tools Used:
 - Programming Language - SQL, PySpark
 - Azure Cloud, Databricks
 - Azure Data lake Gen 2

## Dataset Used:
ERGAST API - Formula 1 Dataset - Drivers and Results JSON files

