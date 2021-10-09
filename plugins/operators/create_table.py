from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults
import sqlTemplate
import os
import psycopg2
from airflow.models.connection import Connection


class CreateTableOperator(BaseOperator):


    """
        Description:

            This custom function creates the required tables
            in redshift database for analytical operation
            by reading the create_table SQL file.

        Arguments:


            sql : an SQL file path.


        Returns:
            None
    """


    ui_color = '#F9BD9E'
    
    template_fields = ("sql",)


    @apply_defaults
    def __init__(self,
                 sql = "",*args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)

        self.sql = sql



    def execute(self, context):
        # redshift connector object
        conn =  context['ti'].xcom_pull(task_ids ='create_redshift')

        try:

            self.log.info("starting to read the sql file")
            fd = open(self.sql, 'r')
            sqlFile = fd.read()
            self.log.info("file path = {v1}, content of the file to proccess {v2}".format(v1=self.sql,
            v2=sqlFile))
            fd.close()
            self.log.info("concluded reading the sql file")
        except:
            self.log.info("failed to read file ")
        #self.roleArn = context['ti'].xcom_pull(task_ids ='create_roles')
        #aws_hook = AwsHook(self.aws_credentials)
        #credentials = aws_hook.get_credentials()
        #redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        try:
            self.log.info("connecting to the database")
            conn = psycopg2.connect("host={v1} dbname={v2} user={v3} password={v4} port={v5}".format(v1=conn.host,
            v2=conn.schema,v3=conn.login,v4=conn.password,v5=conn.port))

            redshift = conn.cursor()

            redshift.execute(sqlFile)
            redshift.close()
            conn.close()
            self.log.info("finished execution and closing the database")

        except (Exception, psycopg2.DatabaseError) as error:
            self.log.info("failed to proccess create table request: {} ".format(error))
