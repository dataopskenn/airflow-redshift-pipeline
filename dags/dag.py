from helpers import SqlQueries
from plugins.operators import CreateTableOperator, DataQualityOperator, LoadDimTableOperator, LoadFactTableOperator, StagingOperator
from subdags import load_dim_subdag
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator


AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')


s3_bucket = 's3_bucket_name'
data_s3_bucket = 'actual_data'
log_s3_key = 'log_data'
log_json_file = 'log_json_path.json'


# Initialize default arguments for DAG job
default_args = {
    'owner': 'owner_name',
    'depends_on_past': True,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'catchup': True
}


dag_name = 'template_dag'
dag = DAG(
    dag_name, 
    default_args = default_args,
    description = 'BigData ELT in Redshift with Airflow',
    schedule_interval = '0,* * * *',
    max_active_runs = 1,
    max_active_tasks = 1
    )


start_operator = DummyOperator(task_id = 'Start_execution', dag = dag)


creat_table_in_redshift = CreateTableOperator(
    task_id = 'create_tables',
    redshift_connect_id = 'redshift',
    dag = dag
)


stage_events_to_redshift = StagingOperator(
    task_id = 'stage_events',
    table_name = 'table_name',
    s3_bucket = s3_bucket,
    s3_key = data_s3_key,
    file_format = "JSON",
    log_json_file = log_json_file,
    redshift_connect_id = "redshift",
    aws_credential_id = "aws_credentials",
    dag = dag,
    provide_context = True
)


# it is assumed that there will be just one fact table, even if this is not usually the case
load_fact_table = LoadFactTableOperator(
    task_id = 'load_fact_table',
    redshift_connect_id = 'redshift',
    sql_query = SqlQueries.table_insert,
    dag = dag
)


# repeat this for multiple dimensions table
load_dimension_table = LoadDimTableOperator(
    subdag = load_dim_subdag(
        parent_dag_name = dag_name, 
        task_id = "load_specific_dim_table", 
        redshift_connect_id = "redshift", 
        sql_query = SqlQueries.dim_table_insert, 
        delete_load = True, 
        table_name = "dim_table_name"),
    
    task_id = 'load_dim_table',
    dag = dag,
)



data_quality_check = DataQualityOperator(
    task_id = "data_quality_check",
    redshift_connect_id = 'redshift',
    tables = ["fact_table_name", "dim_table_name"]
)


end_operator = DummyOperator(
    task_id = 'stop_execution',
    dag = dag
)


start_operator >> create_table_in_redshift
creat_table_in_redshift >> [stage_fact_data_to_redshift, stage_events_to_redshift] >> load_fact_table #list was given assuming there are multiple events to stage
load_fact_table >> load_dimension_table >> data_quality_check >> end_operator