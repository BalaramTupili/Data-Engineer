from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    staging_copy = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region '{}'
        JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                  redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 json="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
                
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.region=region
        self.json=json
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        formatted_sql = StageToRedshiftOperator.staging_copy.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json
        )
        redshift.run(formatted_sql)
        self.log.info('StageToRedshiftOperator complemented ')





