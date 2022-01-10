from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.checks = checks
    
    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        for checks in self.checks:
            test_sql = checks.get('test_sql')
            expected_result = checks.get('expected_result')
            comparison = checks.get('comparison')
            records = redshift.get_records(test_sql)[0][0]
            check_expected_result = " {} {} {} ".format(records, comparison, expected_result)
            self.log.info(f"check_expected_result: {check_expected_result}")
            self.log.info(f"SQL: {test_sql}")
            if not check_expected_result:
                raise ValueError(f"Data quality test failed. {test_sql} returned {records} records. ")
            self.log.info(f"Data quality on query: {test_sql} - check passed with {records} records")
