from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):
    """
    The purpose of this operator is to load data from a staging table
    to a dimension table in a database in Redshift.
    Default parameters of this operator:
    - redshift_conn_id: The Airflow connection ID for Redshift
    - table:  The name of the target dimension table in Redshift
    -sql_queries: The SQL query to execute to extract the data from the staging table and transform it for the target dimension table
    -insert_mode: Flag that indicates whether the data should be appended to the table or truncated meaning emptied
    - *args: Variable length argument list
    - **kwargs: Arbitrary keyword arguments
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql_query = "",
                 append_only = True,
                 *args, **kwargs):
        
        """   
        Pass the arguments that were passed above              

        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        super.redshift_conn_id = redshift_conn_id
        super.table = table
        super.sql_query = sql_query
        super.append_only = append_only

   

        
    def execute(self, context):
        """
        This execute method runs the LoadDimensionOperator. As the insert method is truncate it log a message indicating that the dimension table will be truncated as well as truncate the dimension table before loading the data and logging this info.

        """
        redshift=PostgresHook (postgres_conn_id=self.redshift_conn_id)
        if self.append_only == "True":
            self.log.info(f'Truncating {self.table} dimension table')
            redshift.run(f'TRUNCATE TABLE {self.table}')
        self.log.info(f'Loading data into {self.table} dimension table')
        redshift_hook.run(self.sql_query)
        self.log.info(f'{self.table} dimension table loaded successfully')
