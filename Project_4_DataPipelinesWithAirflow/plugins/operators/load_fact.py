from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    This class contains a method, which loads data from staging tables into fact table.
    
    params:
    :redshift_conn_id: Airflow connection_id to redshift database
    :destination_table: fact table to update
    :sql_statement: 'select' query, which retrieves rows for insertion into fact table
    
    returns: None
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 destination_table = "",
                 sql_statement = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.destination_table=destination_table
        self.sql_statement=sql_statement

    def execute(self, context):
        self.log.info('hooking redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Loading fact table')
        insert_query = 'INSERT INTO {} ({})'.format(self.destination_table, self.sql_statement)
        redshift.run(insert_query)
