from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.append_data = append_data
    
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_data:
            sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql_query)
            redshift.run(sql_statement)
        else:
            sql_statement = 'DELETE FROM %s' % self.table
            redshift.run(sql_statement)
            sql_statement = 'INSERT INTO %s %s' % (self.table, self.sql_query)
            redshift.run(sql_statement)
        records = redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")
        self.log.info(f"Total records from {self.table}: {records[0][0]}")