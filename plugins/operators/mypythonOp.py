from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
import os



class MyPythonOperator(PythonOperator):
    template_fields = ('templates_dict','op_args','op_kwargs')
