from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
class LoadFactOperator(BaseOperator):
    
    """
    The purpose of this operator is to load data into the fact table songplay in Redshift.
    Default parameters of this operator are:
    - redshift_conn_id: The Airflow connection ID for Redshift
    - table: The name of the fact table in Redshift
    - sql_query: The SQL query to use to load the fact table
    - insert_mode: we want the data to be appended to the data in the table
    """
    
    ui_color = '#F98866'
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql_query='',
                 append_only = False,
                 *args, **kwargs):
                 
        """
        Passes the default arguments above
    
        """
        
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.append_only = append_only
        
        
 
        
    def execute(self, context):
        """
        The LoadFactOperator will connect to the Redshift cluster and as the insert_mode is append, it will log a message stating that the data will be loaded and error messages as well as  run an INSERT INTO command and also a SQL query to select which data to be inserted
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append_only:
            self.log.info(f'Load fact table {self.table}')
        redshift.run(f'INSERT INTO {self.table} {self.sql_query}')
    
