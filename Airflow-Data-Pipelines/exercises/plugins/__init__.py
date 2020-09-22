from airflow.plugins_manager import AirflowPlugin

import operators


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin" # You must always give a plugin name
    operators = [
        operators.FactsCalculatorOperator,
        operators.HasRowsOperator,
        operators.S3ToRedshiftOperator
    ]
