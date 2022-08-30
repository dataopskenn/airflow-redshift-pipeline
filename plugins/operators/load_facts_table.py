from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class LoadFactTableOperator(BaseOperator):

    ui_color = ' '


    @apply_defaults

    def __init__(
        self, 
        redshift_connect_id = " ",
        sql_query = " ",
        *args, **kwargs):

        super(LoadFactTableOperator, self).__init__(*args, **kwargs)
        self.redshift_connect_id = redshift_connect_id
        self.sql_query = sql_query


    def execute(self, context):

        redshift_hook = PostgresHook(postgres_connect_id = self.redshift_connect_id)
        redshift_hook.run(self.sql_query)