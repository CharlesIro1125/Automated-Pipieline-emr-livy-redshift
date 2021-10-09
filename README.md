# Automated-Pipieline-emr-livy-redshift
concurrent spark context with redshift star schema

## Achitecture

<img src="https://github.com/CharlesIro1125/Automated-Pipieline-emr-livy-redshift/blob/master/architecture.png" alt="schema" width="800" height="400" />

Pipeline Consist of various modules
- EMR Cluster with Spark and Livy
- Data Warehouse 
- Analytics Module

Overview

The IAM roles and EMR cluster with two EC2 nodes are first created, After which Data is read from an S3 bucket processing source to an EMR cluster for processing. To enable parallel processing of spark jobs, AWS Livy is used to create various spark context for spark job processing. After the data processing is done, the resulted parquest file is loaded to an S3 bucket called the processing sink. Afterwhich a Redshift cluster is created for the data warehouse. Data is then moved from an S3 processing sink to a staging area in Redshift having the required created tables. The data is then transformed from the staged area to a Star schema required for analytical processing. To verify the data integrity after completing this ETL pipeline, some analytic queries are run on the relational data.  

**Note: the created EMR cluster, Redshift cluster and AWS roles are deleted after the execution of the last DAG**

Automated pipeline

<img src="https://github.com/CharlesIro1125/Automated-Pipieline-emr-livy-redshift/blob/master/dagprocess_view.jpg" alt="schema" width="1000" height="400" />
