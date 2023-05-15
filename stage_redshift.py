from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults




class StageToRedshiftOperator(BaseOperator):
    
    """
This operator is used to stage the data from S3 to Redshift.
The parameters are as follows:
- template_fields: templated field that allows to load timestamped files from S3 based on the execution time and run backfills.  
- copy_sql: SQL statement that copies from S3 to Redshift.

    """
    ui_color = '#358140', 
    template_fields = ("s3_key")
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TIMEFORMAT AS '{}'
        REGION '{}'
        FORMAT AS JSON '{}'
        IGNORE HEADER '{}'
    """

    """
    Arguments:
    - redshift_conn_id: Connection ID for the Redshift database
    - aws_credentials: Connection ID for the AWS credentials
    - table: name of the table where the data will be loaded to
    - s3_bucket: name of the S3 bucket where the data is stored
    - s3_key: key of the s3 object to load into Redshift
    - delimiter: delimiter used in the data file
    - ignore_headers: the number of header rows to ignore
    - json_format: specifies the json format for the data that will be copied
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 json_format="",
                 *args, **kwargs):
        
        """
    Pass the arguments that were passed above
 
        """     
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.jason_format = json_format
        self.ignore_headers = ignore_headers
        
    
     def execute(self, context):
            
            """
            Definition of the execute method for this operator by retrieving the AWS credentials with an AWS hook and use PostGres Hook to connect to Redshift.
            """
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

            """
            Log information informing that the operator is being executed as well as logging when the data is being cleared from the destination redshift table in order to ensure that the destination table is empty and also delete all data from the destination table. 
            """


            self.log.info('StageToRedshiftOperator not implemented yet')
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

            """
            Information when the operator is staging the data from S3 to Redshift. 
            The rendered_key is a variable that allows the operator to load data from a different S3 object for each DAG run. 
            The s3_path represents the path to the object being copied.
            The formatted_sql is the SQL query that will copy the data from S3 by  using the format method to replace the placeholders in the copy_sql query with real values and the run via a PostGres hook.
            """
            self.log.info("Staging data from S3 to Redshift")
            self.log.info(f"{num_rows} rows affected by SQL query")
            rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
            formatted_sql = S3ToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )
            redshift.run(formatted_sql)









        
        
    

    


