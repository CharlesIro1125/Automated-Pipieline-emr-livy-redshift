from airflow.operators.postgres_operator import PostgresOperator
from airflow import DAG
from airflow.operators import DataQualityOperator
from airflow.operators.dummy_operator import DummyOperator



def dataQualityCheck(parent_dag_name, task_id,start_date,schedule_interval,*args,**kwargs):



    dag = DAG("{}.{}".format(parent_dag_name,task_id),start_date=start_date,\
                schedule_interval=schedule_interval,**kwargs)

    run_quality_checks_dimStateDestination = DataQualityOperator(
        task_id = "Run_data_quality_checks_dimStateDestination",
        dag = dag,
        schema = "public",
        table = "dimStateDestination"
        )

    run_quality_checks_dimArriveDate = DataQualityOperator(
        task_id = "Run_data_quality_checks_dimArriveDate",
        dag = dag,
        schema = "public",
        table = "dimArriveDate"
        )

    run_quality_checks_dimAirport = DataQualityOperator(
        task_id = "Run_data_quality_checks_dimAirport",
        dag = dag,
        schema = "public",
        table = "dimAirport"
        )

    run_quality_checks_dimTourist = DataQualityOperator(
        task_id = "Run_data_quality_checks_dimTourist",
        dag = dag,
        schema = "public",
        table = "dimTourist"
        )
    run_quality_checks_factTable = DataQualityOperator(
        task_id = "Run_data_quality_checks_factTable",
        dag = dag,
        schema = "public",
        table = "factArrivetheUsa"
        )


    start_check = DummyOperator(task_id = 'Begin_check',  dag = dag)

    check_passed = DummyOperator(task_id = 'Check_successful',  dag = dag)


    start_check >> [run_quality_checks_factTable,run_quality_checks_dimTourist,run_quality_checks_dimAirport,
    run_quality_checks_dimArriveDate,run_quality_checks_dimStateDestination] >> check_passed

    return dag
