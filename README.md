# Automated-Pipieline-emr-livy-redshift
concurrent spark context with Emr-Livy and redshift star schema

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

<img src="https://github.com/CharlesIro1125/Automated-Pipieline-emr-livy-redshift/blob/master/dagprocess_view.jpg" alt="schema" width="1200" height="300" />

Environment Set-up

```
  EMR - 1 master node
    release_label='emr-5.9.0'
    master_instance_type='m4.xlarge'
  
  EC2 - 1 slave node
    core_node_instance_type='m4.xlarge'
  
  Redshift - 1 node
    cluster_type='multi-node'
    node_type='dc2.large'
  
  Python version
    python 2.7
   
  Airflow backend database
    postgresql (metadata database)
    rabbitmq (Celery result backend)
    
  Airflow version
    version 1.10.15
    
  Celery version
    version 4.3.1
      
```
to setup the required backend see [set-up backend](https://medium.com/@ryanroline/installing-apache-airflow-on-windows-10-5247aa1249ef)

Setting Up Airflow

This project uses some custom opereators as well as Airflow operators. Airflow variables are used to hold some static values required for authentication to an AWS account. 
An Access key and Secret key, Cluster parameters, AWS region, e.t.c.

Airflow Variables

<img src="https://github.com/CharlesIro1125/Automated-Pipieline-emr-livy-redshift/blob/master/dagvariables.jpg" alt="schema" width="1200" height="300" />

The variables are used to create the required resources. Since the Redshift cluster is created within the pipeline, a postgre psycopg2 database connector is used to establish connection to the Redshift database. 

An AWS Infrastructure as code module 'boto3' is used to build the required resources. Both boto3 and psycopg2 are python modules that needs to be installed.
To install modules on python 2.7.

```
  pip install psycopg2
  pip install boto3
```

The project contains some major folders and Files.

- The helpers folder, containing the sql ingestion code, to insert data into the database
- The operators folder, containing all the custom operators used in the project.
- The Airflow lib folder, containing the Emr and Redshift file for creating the clusters and an IAM file for creating the required roles to be assumed by the created                resources.
- The Sparkjob folder, containing the spark scritps to be posted to the EMR cluster using the Livy API post request.
- The sqlTemplate folder, containing the SQL create table statements.
- The main_dag_run file, the entry point for executing the Dags and all processes.
- The subdag file, for sub-processes to the main Dag.

## Running the Dag.
To start Airflow after installation and starting the backend database

```
  Start airflow
    airflow db init
  Start the Web server on desired port (8081 used)
    airflow webserver -p 8081
  Start airflow scheduler
    airflow scheduler
  Start airflow executor
    airflow celery worker    
```    
  
Once the dag is ready and avaliable from the Airflow Web UI, it runs according to defined start date and does a back-filling if the start date is in the past.

A completed Dag run, with start date set to 'two days ago'.

Dag graph view

<img src="https://github.com/CharlesIro1125/Automated-Pipieline-emr-livy-redshift/blob/master/dagprocess_1.jpg" alt="schema" width="1200" height="400" />

Dag tree view

<img src="https://github.com/CharlesIro1125/Automated-Pipieline-emr-livy-redshift/blob/master/dagcombine.jpg" alt="schema" width="600" height="400" />

## The Analytics Module (black colored task above).
For data integrity check, an analytical query is run on the relations to verify its consistance with the expected data quality and its result is logged in a log folder. This check is done within the pipeline with a custom operator called Data Quality Check .

Data_qualtiy_check graph view.

<img src="https://github.com/CharlesIro1125/Automated-Pipieline-emr-livy-redshift/blob/master/dagprocess_4.jpg" alt="schema" width="1000" height="400" />

Data_qualtiy_check tree view.

<img src="https://github.com/CharlesIro1125/Automated-Pipieline-emr-livy-redshift/blob/master/dagprocess_3.jpg" alt="schema" width="1000" height="400" />

## Example queries done on the star Schema.

Star Schema

<img src="https://github.com/CharlesIro1125/DataWarehouse/blob/master/CapstoneProject/UdacityCapstone.jpg" alt="schema" width="800" height="600" />
