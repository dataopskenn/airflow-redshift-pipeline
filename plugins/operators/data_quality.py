from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class DataQualityOperator(BaseOperator):

    ui_color = ' '


    @apply_defaults
    
    def __init__(self, redshift_connect_id = " ", tables = [], *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_connect_id = redshift_connect_id
        self.tables = tables


    def execute(self, context):

        redshift_hook = PostgresHook(postgres_connect_id = self.redshift_connect_id)

        for table_name in self.tables:

            self.log.info(f"Validating data quality on the {table_name} table_name")
            records = redshift_hook.get_records(f"select count(*) from {table_name};")

            if len(records) < 1 or len(records[0] < 1 or len(records[0][0] < 1)):
                self.log.error(f"Failed Data Quality Validation for the {table_name} table_name")
                raise ValueError(f"Failed Data Quality Validation for the {table_name} table_name")
            self.log.info(f"Passed Data Quality Validation for the {table_name} table_name")

        self.log.info("Yet to execute DataQualityOperator")