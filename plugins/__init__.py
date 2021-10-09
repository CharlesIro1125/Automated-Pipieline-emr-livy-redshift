from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin
import operators
import helpers

# Defining the plugin class
class IroCharlesPlugin(AirflowPlugin):

    name = "irocharles_plugin"

    operators = [
        operators.MyPythonOperator,
        operators.DataQualityOperator,
        operators.LoadDimensionOperator,
        operators.LoadFactOperator,
        operators.StageToRedshiftOperator,
        operators.CreateTableOperator

    ]

    helpers = [
        helpers.SqlQueries
    ]
