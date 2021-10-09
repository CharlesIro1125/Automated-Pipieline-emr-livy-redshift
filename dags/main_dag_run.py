
import boto3, json, pprint, requests, textwrap, time, logging
import requests
from airflow.models import Variable
import os
import logging
from datetime import datetime, timedelta, date
from airflow import DAG
import airflowlib.emr_lib as emr_lib
import airflowlib.IAM_lib as IAM_lib
import sqlTemplate
from airflow.operators.python_operator import PythonOperator
from airflow.operators import MyPythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models.connection import Connection
#from airflow.contrib.hooks.ssh_hook import SSHHook
#from decouple import config
#from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
#from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.subdag_operator import SubDagOperator
from subdag import dataQualityCheck
from helpers import SqlQueries
import configparser
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import CreateTableOperator




config =configparser.ConfigParser()


start_date = days_ago(2)
default_args = {
    'owner': 'airflow',
    'depends_on_past':True,
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(minutes=0.5),
    'execution_timeout':timedelta(seconds=1000),
    'provide_context': True
}


dag = DAG('online_analytical_proccessing', concurrency=3, schedule_interval='01 0 * * *', default_args=default_args)


dag_config = Variable.get("dag_config", deserialize_json = True)

KEY = Variable.get("KEY")
SECRET = Variable.get("SECRT")

os.environ["AWS_ACCESS_KEY_ID"]=KEY
os.environ["AWS_SECRET_ACCESS_KEY"]=SECRET

db_user = dag_config["db_user"]
db_name = dag_config["db_name"]
db_port = dag_config["port"]
root = dag_config["root"]
db_password = dag_config["db_password"]
redshiftCluster_id = dag_config["dwh_cluster_id"]
node_count = dag_config["dwh_num_nodes"]
dwh_node_type = dag_config["dwh_node_type"]
rolename = dag_config["role_name"]
InstancePrfName = dag_config["InstancePrfName"]
region_name = dag_config["region_name"]
EmrEC2Role_name = dag_config["EmrEC2Rolename"]
RedshiftRole_name = dag_config["RedshiftRolename"]



def create_roles(**kwargs):
    # create roles for permittion to aws services
    roleArn = IAM_lib.create_role(Rname=rolename)
    EmrEc2roleArn = IAM_lib.create_EMREC2role(Rname=EmrEC2Role_name)
    Redshift_roleArn = IAM_lib.redshift_role(Rname=RedshiftRole_name)
    IAM_lib.create_Attach_instanceProfileForEc2(InstancePrfName,Rname=EmrEC2Role_name)
    os.environ["RoleArn"] = roleArn
    Roles = {'roleArn':roleArn,'EmrEc2roleArn':EmrEc2roleArn,'Redshift_roleArn':Redshift_roleArn}
    return Roles



def create_emr(**kwargs):
    # create an emr cluster and return it's cluster id
    cluster_id = emr_lib.list_clusters()
    if cluster_id == 'None':
        cluster_id = emr_lib.create_cluster(InstancePrfName,rolename)
        emr_lib.wait_for_emr_cluster_creation(cluster_id)
        return cluster_id
    else:
        return cluster_id



def create_redshift(**kwargs):
    # creates a redshift cluster and return it's endpoint
    roleArn = kwargs['ti'].xcom_pull(task_ids='create_roles')
    roleArn = roleArn['Redshift_roleArn']
    response = emr_lib.create_redshfit_cluster(roleArn,db_name,redshiftCluster_id,db_user,db_password,node_count)
    emr_lib.wait_for_redshift_creation(redshiftCluster_id)
    endpoint = emr_lib.get_redshift_endpoint(redshiftCluster_id)

    conn = Connection(
        conn_id='postgres_conn_id',
        conn_type='postgres',
        host=endpoint,
        login=db_user,
        port =db_port,
        password=db_password,
        schema = db_name
        )

    return conn

def process_data(code,**kwargs):
    # parallel spark context for data processing with levy
    cluster_id = kwargs['ti'].xcom_pull(task_ids='create_emr')
    master_dns = emr_lib.get_emr_cluster_dns(cluster_id)
    response_headers = emr_lib.create_spark_session(master_dns)
    session_url = emr_lib.wait_for_idle_session(master_dns, response_headers)
    statement_response = emr_lib.submit_statement(session_url,root + code)
    emr_lib.track_statement_progress(master_dns, statement_response.headers)
    emr_lib.kill_spark_session(session_url)


def terminate_emr(**kwargs):
    # terminate emr cluster after the last execution
    ds = kwargs['execution_date'].add(days=1)
    dt = datetime.now()
    if  dt.hour >= ds.hour and dt.day == ds.day:
        logging.info("terminating the emr cluster")
        cluster_id = kwargs['ti'].xcom_pull(task_ids ='create_emr')
        emr_lib.terminate_emr_cluster(cluster_id)
    else:
        logging.info("cluster is running")
        pass


def delete_redshift(**kwargs):
    # terminate redshift cluster after the last execution
    ds = kwargs['execution_date'].add(days=1)
    dt = datetime.now()
    if  dt.hour >= ds.hour and dt.day == ds.day:
        logging.info("terminating the redshift cluster")
        emr_lib.delete_redshift(redshiftCluster_id)
    else:
        logging.info("cluster is running")
        pass




def terminate_roles(**kwargs):
    # terminate roles after the last execution
    ds = kwargs['execution_date'].add(days=1)
    dt = datetime.now()
    if  dt.hour >= ds.hour and dt.day == ds.day:
        logging.info("deleting the created roles")
        IAM_lib.delete_instanceProfile(InstancePrfName,Rname=EmrEC2Role_name)
        IAM_lib.delete_redshift_role(RedshiftRole_name)
        IAM_lib.delete_created_role(rolename)
        IAM_lib.delete_created_EMREC2role(EmrEC2Role_name)
    else:
        logging.info("roles set and available")
        pass

createRoles = PythonOperator(
    task_id = 'create_roles',
    python_callable = create_roles,
    dag=dag)

create_emr = PythonOperator(
    task_id = 'create_emr',
    python_callable = create_emr,
    dag=dag)


terminate_CreatedEmr = PythonOperator(
    task_id = 'terminate_emr',
    python_callable =terminate_emr,
    dag=dag)


delete_CreatedRedshift = PythonOperator(
    task_id = 'delete_redshift',
    python_callable = delete_redshift,
    dag = dag)


terminateRoles = PythonOperator(
    task_id = 'terminate_roles',
    python_callable = terminate_roles,
    dag=dag)


process_citydemographyData = MyPythonOperator(
    task_id='process_citydemographyData',
    python_callable = process_data,
    provide_context = True,
    op_kwargs = {'code': 'citydemography.py'},
    dag = dag)


process_airportData = MyPythonOperator(
    task_id ='process_airportData',
    python_callable = process_data,
    provide_context=True,
    op_kwargs= {'code': 'airportcode.py'},
    dag = dag)


process_immigrationData = MyPythonOperator(
    task_id = 'process_immigrationData',
    python_callable = process_data,
    provide_context = True,
    op_kwargs = {'code': 'immigration.py'},
    dag = dag)


process_tempData = MyPythonOperator(
    task_id = 'process_tempData',
    python_callable = process_data,
    provide_context = True,
    op_kwargs = {'code': 'temperature.py'},
    dag = dag)



createRedshift = PythonOperator(
    task_id = 'create_redshift',
    python_callable = create_redshift,
    dag = dag)


create_tables =CreateTableOperator(
    task_id = 'create_tables_in_redshift',
    dag = dag,
    sql = "dags/sqlTemplate/create_tables.sql"
)


staging_immigration_to_redshift = StageToRedshiftOperator(
    task_id = "staging_immigration_data",
    dag = dag,
    aws_key = KEY,
    aws_secret = SECRET,
    target_table = "staging_immigration",
    schema = "public",
    file_format = "parquet",
    s3_bucket = "charlesprojects-sink",
    s3_key = "immigrationData",
    region = region_name

)


staging_airport = StageToRedshiftOperator(
    task_id = "staging_airport_data",
    dag = dag,
    aws_key =KEY,
    aws_secret = SECRET,
    target_table = "staging_airport",
    schema = "public",
    file_format = "parquet",
    s3_bucket = "charlesprojects-sink",
    s3_key = "airportData",
    region = region_name

)

staging_demography_to_redshift = StageToRedshiftOperator(
    task_id = "staging_demography_data",
    dag = dag,
    aws_key = KEY,
    aws_secret = SECRET,
    target_table = "staging_demography",
    schema = "public",
    file_format = "parquet",
    s3_bucket = "charlesprojects-sink",
    s3_key = "demographyData",
    region = region_name

)

staging_landtemp_to_redshift = StageToRedshiftOperator(
    task_id = "staging_landtemp_data",
    dag = dag,
    aws_key = KEY,
    aws_secret = SECRET,
    target_table = "staging_landtemp",
    schema = "public",
    file_format = "parquet",
    s3_bucket = "charlesprojects-sink",
    s3_key = "landtempData",
    region = region_name
)

startAnalyticalSchema= DummyOperator(task_id = 'startAnalyticalSchema',  dag = dag)
#S3_sink = DummyOperator(task_id = 'drop_to_S3_sink',  dag = dag)

factTableArrivetheUsa = LoadFactOperator(
    task_id = 'factTableArrivetheUsa',
    dag = dag,
    sql_statement = SqlQueries.factArrivetheUsa_insert,
    schema = "public",
    table = "factArrivetheUsa",
    params = {'temp_table':'temp_factArrivetheUsa'}
)

dimTableStateDestination = LoadDimensionOperator(
    task_id = 'dimTableStateDestination',
    dag = dag,
    sql_statement = SqlQueries.dimStateDestination_insert,
    schema = "public",
    table = "dimStateDestination",
    params = {'temp_table':'temp_dimStateDestination'}
)

dimTableTourist = LoadDimensionOperator(
    task_id = 'dimTableTourist',
    dag = dag,
    sql_statement = SqlQueries.dimTourist_insert,
    schema = "public",
    table = "dimTourist",
    params = {'temp_table':'temp_dimTourist'}
)

dimTableArriveDate= LoadDimensionOperator(
    task_id = 'dimTableArriveDate',
    dag = dag,
    sql_statement = SqlQueries.dimArriveDate_insert,
    schema = "public",
    table = "dimArriveDate",
    params = {'temp_table':'temp_dimArriveDate'}
)

dimTableAirport = LoadDimensionOperator(
    task_id = 'dimTableAirport',
    dag = dag,
    sql_statement = SqlQueries.dimAirport_insert,
    schema = "public",
    table = "dimAirport",
    params = {'temp_table':'temp_dimAirport'}
)



Run_data_quality_check = SubDagOperator(
    subdag = dataQualityCheck(parent_dag_name = 'online_analytical_proccessing', task_id = "data_quality_check",
                               start_date = start_date,schedule_interval = dag.schedule_interval),
    task_id = "data_quality_check",
    dag = dag
)



createRoles >> create_emr

create_emr >> process_airportData >> createRedshift
create_emr >> process_citydemographyData >> createRedshift
create_emr >> process_immigrationData >> createRedshift
create_emr >> process_tempData >> createRedshift

createRedshift >> create_tables >> [staging_immigration_to_redshift, \
 staging_landtemp_to_redshift,staging_demography_to_redshift,staging_airport] >> startAnalyticalSchema >> [factTableArrivetheUsa, \
 dimTableStateDestination,dimTableTourist,dimTableArriveDate,\
 dimTableAirport] >> Run_data_quality_check >> terminate_CreatedEmr >> delete_CreatedRedshift >> terminateRoles
