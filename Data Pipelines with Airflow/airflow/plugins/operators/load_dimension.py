from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_statement="",
                 is_append=False,
                 table_name = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_statement=sql_statement
        self.is_append=is_append
        self.table_name = table_name

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.is_append:
            sql_statement = "INSERT INTO {} {}".format(self.table_name, self.sql_statement)
            redshift.run(sql_statement)
        else:
            sql_statement = "TRUNCATE TABLE  {}".format(self.table_name)
            redshift.run(sql_statement)
            sql_statement = "INSERT INTO {} {}".format(self.table_name, self.sql_statement)
            redshift.run(sql_statement)

        self.log.info("loading data into {} Dimension table completed".format(self.table_name))
