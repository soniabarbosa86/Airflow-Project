from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
"""
The purpose of this operator is to run quality checks (dq_checks defined in the DAG) in the Redshift database. The parameters are as follows:
- dq_checks: list of data quality checks defined in the DAG.
- redshift_conn_id: connection ID for the Redshift cluster.

"""
    ui_color = '#89DA59'
    @apply_defaults
    def __init__(self,
                 dq_checks=[],
                 redshift_conn_id="",
                 *args, **kwargs):
"""  
Passes on the arguments from the parent class above

"""

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id
        

    def execute(self, context):
        redshift=PostgresHook (postgres_conn_id=self.redshift_conn_id)

"""
for loop that iterates each data quality check by using get method to retrieve the SQL query and expected_result keys.

"""
        
        for check in self.dq_checks_
        sql = check.get('check_sql')
        expected_result = check.get('expected_result')

"""
This part of the exectute process checks whether any rows or columns were returned. If not an error will be logged.
"""

        records = redshift_hook.get_records(sql)
        if len(records) < 1 or len(records[0]) <1:
            raise ValueError(f'Data quality check failed. {sql} returned no results.")
 
 """
 This part of the process compares the actual result to the expected result defined in dq_checks. If this is not the case an error will be logged.
 
 """
        actual_result = records[0][0]
        if actual_result != expected_result:
            raise ValueError(f"Data quality check failed. {sql} returned {actual_result} but                     expected {expected_result}.")
        
        self.log.info(f"Data quality check passed for {sql}")
                             
                             
                                    
        
