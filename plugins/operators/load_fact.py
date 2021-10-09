from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2

class LoadFactOperator(BaseOperator):

    """
        Description:

            This custom function loads data into the
            fact table by either appending the data into
            the table or truncating the table before
            inserting.

        Arguments:

            redshift_conn_id : the connection id to the cluster .
            schema : the table schema.
            table : the table name.
            sql_statement : an sql select statement
            operation : either to append or truncate_insert,
                        default is set to append.
            params : a temporary table created in the database,
                     use for append operation.

        Returns:
            None

    """


    ui_color = '#F98866'


    @apply_defaults
    def __init__(self,
                 sql_statement = "",
                 schema = "",
                 operation = "append",
                 table = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql_statement = sql_statement
        self.schema = schema
        self.table = table
        self.operation = operation
        self.temp_table = kwargs['params']['temp_table']

    def execute(self, context):

        self.log.info('implementing LoadFactOperator')
        #redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        conn =  context['ti'].xcom_pull(task_ids ='create_redshift')
        print("connecting to the database")
        conn = psycopg2.connect("host={v1} dbname={v2} user={v3} password={v4} port={v5}".format(v1=conn.host,
        v2=conn.schema,v3=conn.login,v4=conn.password,v5=conn.port))

        redshift = conn.cursor()

        if self.operation == "append":

            sql_temp_table = """
                BEGIN;
                TRUNCATE {v1}.{v2};
                INSERT INTO {v1}.{v2} {v3};
                COMMIT;
               """.format(v1=self.schema,v2=self.temp_table,v3=self.sql_statement)
            redshift.execute(sql_temp_table)
            ######conn.commit()

            sql_append = """
                ALTER TABLE {v1}.{v2} APPEND
                FROM {v1}.{v3}
                FILLTARGET
                """.format(v1=self.schema,v2=self.table,v3=self.temp_table)
            redshift.execute(sql_append)
            conn.commit()

        if self.operation == "truncate_insert":

            sql_insert_table = """
                BEGIN;
                TRUNCATE {v1}.{v2};
                INSERT INTO {v1}.{v2} {v3};
                COMMIT;
               """.format(v1=self.schema,v2=self.table,v3=self.sql_statement)
            redshift.execute(sql_insert_table)
            conn.commit()
