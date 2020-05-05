from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql="",
                 table="",
                 append_data=None,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.sql=sql
        self.table=table
        self.append_data=append_data

    def execute(self, context):
        self.log.info('In load_dimension: ')
        #aws_hook = AwsHook(self.aws_credentials_id)
        #credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data == True:
            sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql)
            redshift.run(sql_statement)
        else:
            sql_statement = 'DELETE FROM %s' % self.table
            redshift.run(sql_statement)
            sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql)
            redshift.run(sql_statement)
        
        self.log.info('load_dimension is done')