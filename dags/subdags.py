from helpers import SqlQueries
from plugins.operators.load_dims_table import LoadDimTableOperator
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


def load_dim_subdag(
    parent_dag_name,
    task_id,
    redshift_connect_id,
    sql_query,
    delete_load,
    table_name,
    *args, **kwargs
    ):

    dag = DAG(f"parent_dag_name.task_id", **kwargs)

    load_dim_table = LoadDimTableOperator(
        task_id = task_id,
        dag = dag,
        redshift_connect_id = redshift_connect_id,
        sql_query = sql_query,
        delete_load = delete_load,
        table_name = table_name
    )

    load_dim_table

    return dag