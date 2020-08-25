from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 load_sql="",
                 table="",
                 insert_mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_sql = load_sql
        self.table = table
        self.insert_mode = insert_mode


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading into {} dimension table".format(self.table))
        self.log.info("Insert mode: {}".format(self.insert_mode))
        if (self.insert_mode): 
            sql_statement = 'INSERT INTO %s %s' % (self.table, self.load_sql)
            redshift.run(sql_statement)
        else:
            sql_del_statement = 'DELETE FROM %s' % (self.table)
            redshift.run(sql_del_statement)
            sql_statement = 'INSERT INTO %s %s' % (self.table, self.load_sql)
            
        redshift.run(sql_statement)
