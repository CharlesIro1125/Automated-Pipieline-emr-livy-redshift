from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults
import os
import psycopg2


class StageToRedshiftOperator(BaseOperator):


    """
        Description:

            This custom function extracts data from the
            s3 storage to the database for analytical operation.

        Arguments:

            redshift_conn_id : the connection id to the cluster.
            schema : the table schema.
            sql_statement : an sql selct statement.
            aws_credentials : the aws credentials.
            target_table : the staging table in the database.
            file_format : the file formate, either json or csv.
            s3_bucket : the s3 bucket name.
            s3_key : the s3 key prefix.
            delimeter : the csv delimiter, for csv file.
            ignore_header : either to ignore the header or not.
            region : the region of the database.

        Returns:
            None
    """


    ui_color = '#89DA59'

    template_fields = ("s3_key",)


    @apply_defaults
    def __init__(self,
                 aws_key = "",
                 aws_secret ="",
                 target_table = "",
                 schema = "",
                 file_format = "",
                 s3_bucket = "",
                 s3_key = "",
                 ignore_header = 1,
                 region = "",*args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.target_table = target_table
        self.schema = schema
        self.file_format = file_format
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_header = ignore_header
        self.region = region

        #self.host_endpoint = kwargs['ti'].xcom_pull(task_ids ='create_redshift')


    def execute(self, context):

        self.log.info('implementing StageToRedshiftOperator')

        roleArn = context['ti'].xcom_pull(task_ids='create_roles')
        roleArn = roleArn['Redshift_roleArn']

        conn =  context['ti'].xcom_pull(task_ids ='create_redshift')
        #self.roleArn = context['ti'].xcom_pull(task_ids ='create_roles')
        #aws_hook = AwsHook(self.aws_credentials)
        #credentials = aws_hook.get_credentials()
        #redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        #conn = psycopg2.connect("host={v1} dbname={v2} user={v3} password={v4} port={v5}".format(v1=self.host_endpoint,
        #v2=self.dag_config["db_name"],v3=self.dag_config["db_user"],v4=self.dag_config["db_password"],
        #v5=self.dag_config["db_port"]))

        #try:

        print("connecting to the database")
        conn = psycopg2.connect("host={v1} dbname={v2} user={v3} password={v4} port={v5}".format(v1=conn.host,
        v2=conn.schema,v3=conn.login,v4=conn.password,v5=conn.port))

        redshift = conn.cursor()
        self.log.info("Clearing data from {v1}".format(v1=self.target_table))
        redshift.execute("TRUNCATE {v1}.{v2}".format(v1=self.schema,v2=self.target_table))
        conn.commit()
        s3_path = "s3://{v1}/{v2}/".format(v1=self.s3_bucket,v2=self.s3_key)

        if self.file_format == "json":
            copy_statement = """
                COPY {v1}.{v2} FROM '{v3}'
                ACCESS_KEY_ID '{v4}' SECRET_ACCESS_KEY '{v5}'
                json 'auto ignorecase'  region '{v6}'
                """.format(v1=self.schema,v2=self.target_table,v3=s3_path,v4=self.aws_key,
                v5=self.aws_secret,v6=self.region)
        elif self.file_format == "parquet":
            copy_statement = """
                copy {v1}.{v2} from '{v3}'
                credentials 'aws_iam_role={v4}'
                FORMAT AS PARQUET;""".format(v1=self.schema,v2=self.target_table,v3=s3_path,
                v4=roleArn)
        else:
            return self.log.info("StageToRedshiftOperator allows only parquet or Json file format")


        redshift.execute(copy_statement)
        conn.commit()
        redshift.close()
        conn.close()
        print("finished committing to the database")

        #except:
        self.log.info(self.target_table, "Failed to proccess request")
