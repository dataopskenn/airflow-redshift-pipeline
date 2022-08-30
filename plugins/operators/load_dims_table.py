from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class LoadDimTableOperator(BaseOperator):

    ui_color = ' '


    @apply_defaults

    def __init__(
        self, 
        redshift_connect_id = " ",
        sql_query = " ",
        table_name = " ",
        delete_load = FALSE, 
        *args, **kwargs):

        super(LoadDimTableOperator, self).__init__(*args, **kwargs)
        self.redshift_connect_id = redshift_connect_id
        self.sql_query = sql_query
        self.table_name = table_name
        self.delete_load = delete_load


    def execute(self, context):

        redshift_hook = PostgresHook(postgres_connect_id = self.redshift_connect_id)

        if self.delete_load == TRUE:
            self.log.info("Delete load operation is set to TRUE. Running DELETE query ong the {self.table_name} table... ")
            redshift_hook.run(f"DELETE FROM {self.table_name}")

        self.log.info(f"Loading data into the Dimensions Table {self.table_name}")
        redshift_hook.run(self.sql_query)
        self.log.info(f"Done loading the Dimensions Table {self.table_name}")