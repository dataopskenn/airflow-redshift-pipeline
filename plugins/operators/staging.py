from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook


class StagingOperator(BaseOperator):

    ui_color = ' '

    
    copy_query = " COPY {} \
    FROM '{}' \
    ACCCESS_KEY_ID '{}' \
    SECRET_ACCESS_KEY '{}' \
    FORMAT AS json '{}'; \
    
    "


    @apply_defaults
    
    def __init__(
        self, 
        redshift_connect_id = " ",
        aws_credential_id = " ",
        table_name = " ",
        s3_bucket = " ",
        s3_key = " ",
        file_format = " ",
        log_json_file = " ",
        *args, **kwargs):

        super(StagingOperator, self).__init__(*args, **kwargs)
        self.redshift_connect_id = redshift_connect_id
        self.aws_credential_id = aws_credential_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.log_json_file = log_json_file
        self.execution_date = kwargs.get('execution_date')


    def execute(self, context):

        """
        - Initialize AwsHook
        - Initialize Aws credentials
        - Initialize conditions for log_json_file
        - Execute the function and log the output   

        """

        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()


        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info(f"Selecting a staging file for the table {self.table_name} from location: {s3_path}")


        if self.log_json_file != " ":
            self.log_json_file = "s3://{}/{}".format(self.s3_bucket, self.log_json_file)
            copy_query = self.copy_query.format(self.table_name, self.s3_path, self.credentials.access_key, self.credentials.access_secret, self.log_json_file)
        else:
            copy_query = self.copy_query.format(self.table_name, self.s3_path, self.credentials.access_key, self.credentials.access_secret, 'auto')

        
        self.log.info(f"Running copy_query: {copy_query}")
        redshift_hook = PostgresHook(postgres_connect_id = self.redshift_connect_id)
        redshift_hook.run(self.sql_query)
        self.log.info(f"Successfully staged the {table_name} table")