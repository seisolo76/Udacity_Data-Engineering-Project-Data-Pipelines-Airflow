from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[""],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('DataQualityOperator started')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        dq_Check_List=[ 
            {'table': 'songs',
            'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null",
            'expected_result': 0},
            {'table': 'artists',
            'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null",
            'expected_result':0},
            {'table': 'users',
            'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null",
            'expected_result':0},
            {'table': 'time',
            'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null",
            'expected_result':0}
            ]
        for table in self.tables:
            self.log.info(f"Checking Table {table}")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data Quality check failed. {table} returned no results.. is empty")
            
        for check in dq_Check_List:
            records = redshift.get_records(check['check_sql'])[0]
            if records[0] != check['expected_result']:
                raise ValueError(f"Data Quality Check Failed! {check['table']} contains null value in id column, got {records[0]} instead")
           
            