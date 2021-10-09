from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2

class DataQualityOperator(BaseOperator):

    """
        Description:

            This custom function performs data quality check on the
            dimension tables to verify if the table is empty or
            wasn't returned.

        Arguments:

            redshift_conn_id : the connection id to the cluster .
            schema : the table schema.
            table : the table name

        Returns:
            logs info of the check result.
    """



    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 schema = "",
                 table = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.schema = schema
        self.table = table

    def execute(self, context):
        #redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        #records = redshift.get_records("SELECT COUNT(*) FROM {}.{}".format(self.schema,self.table))
        #conn =  context['ti'].xcom_pull(task_ids ='create_redshift')

        conn =  context['ti'].xcom_pull(task_ids ='create_redshift',dag_id='online_analytical_proccessing')
        print("connecting to the database attempt 2")
        print("connecting to database 2",conn.host,conn.schema)
        conn = psycopg2.connect("host={v1} dbname={v2} user={v3} password={v4} port={v5}".format(v1=conn.host,
            v2=conn.schema,v3=conn.login,v4=conn.password,v5=conn.port))



        redshift = conn.cursor()

        redshift.execute("SELECT COUNT(*) FROM {}.{}".format(self.schema,self.table))

        records = redshift.fetchall()

        rowcount = redshift.rowcount


        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality checked failed.{}.{} returned no results".format(self.schema,self.table))

        if records[0][0] < 1:
            raise ValueError("Data quality checked failed.{}.{} contains 0 records".format(self.schema,self.table))

        self.log.info("Data quality check on table {}.{} passed with {} records".format(self.schema,self.table,records[0][0]))
        print(records)
        print(rowcount)
