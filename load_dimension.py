from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    The purpose of this operator is to load data from a staging table
    to a dimension table in a database in Redshift.
    Default parameters of this operator:
    - redshift_conn_id: The Airflow connection ID for Redshift
    - table:  The name of the target dimension table in Redshift
    -sql_query: The SQL query to execute to extract the data from the staging table and transform it for the target            dimension table
    -append_data: Flag that indicates whether the data should be appended to the table or replaced
    - *args: Variable length argument list
    - **kwargs: Arbitrary keyword arguments
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
