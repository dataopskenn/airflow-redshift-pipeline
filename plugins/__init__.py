from __future__ import division, absolute_import, print_function

import operators
import helpers

from airflow.plugins_manager import AirflowPlugin


# define the plugin class

class PipelinePlugin(AirflowPlugin):

    name = "pipeline_plugin"
    operators = [
        operators.CreateTableOperator,
        operators.LoadFactTableOperator,
        operators.LoadDimTableOperator,
        operators.DataQualityOperator,
        operators.StagingOperator
    ]

    helpers = [
        helpers.SqlQueries
    ]