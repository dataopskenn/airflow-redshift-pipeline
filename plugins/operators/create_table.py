from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class CreateTableOperator(BaseOperator):

    ui_color = ' '


    @apply_defaults
    
    def __init__(self, redshift_connect_id = " ", *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.redshift_connect_id = redshift_connect_id
        

    def execute(self, context):

        self.log.info("Creating Tables In Redshift...")
        query = open('create_tables.sql', 'r').read()
        redshift_hook.run() #take note eo make sure this runs properly

        self.log.info("Done creating tables ")