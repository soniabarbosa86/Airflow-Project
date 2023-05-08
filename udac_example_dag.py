from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import (
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator)
from helpers import SqlQueries

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')
"""
Default values for the arguments of the DAG:
- owner: owner of the DAG
- start_date: start date of the DAG
- email_on_retry: send or not send an email when a task is retried
- retries: amount of times a task to run if it fails
- retry_delay: amount of time between retries
- depends_on_past : if a task depends on the the previous run was ok.

"""
default_args = {
    'owner': 'SoniaBarbosa',
    'start_date': datetime(2019, 1, 12),
    'email_on_retry': False,
    'retries':3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
}

"""
creation of the DAG:
- default_args: inherited values from the above default_args
- description: describes what the DAG does
- schedule_interval: specifies the schedule interval for the DAG. For this project the interval is hourly. 

"""

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

"""
Dummy Operator used to indicate that that the DAG has begun"
"""
start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag,
    )


"""
Operator that stages the data from S3 to Redshift. The parameters are:
- task_id: ID of the task
- dag: the DAG the task belongs to
- table: name of the Redshift table where the data will be copied to
- redshift_conn_id: ID to connect to Redshift
- aws_credentials_id: connection ID to access the S3 bucket
- s3_bucket: name of the S3 bucket
- s3 key: key of the S3 object to copy which is log_data
- log_json_path: path to the json used to extract data from the log files

"""

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    log_json_path = "s3://udacity-dend/log_data"
    
)

"""
Creation of a task and using the arguments found in the the stage_redshift.py file"

"""
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
  )
 
"""
Creation of a task and using the arguments found in the the load_fact.py file"

"""

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
)

"""
Creation of a task and using the arguments found in the the load_dimension.py file"

"""

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.users_table_insert,
    insert_mode='truncate'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.song_table_insert,
    insert_mode='truncate'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.artist_table_insert,
    insert_mode='truncate'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.time_table_insert,
    insert_mode='truncate'
)

"""
Creation of a task and using the arguments found in the the data_quality.py file"

"""
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

"""
Dummy Operator used to indicate that that the DAG run has ended"
"""
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

"""
Ordering of the tasks
"""

Begin_execution >> Stage_events
Begin_execution >> Stage_songs
Stage_events >> Load_songplays_fact_table
Stage_songs >> Load_songplays_fact_table
Load_songplays_fact_table >> Load_song_dim_table
Load_songplays_fact_table >> Load_user_dim_table
Load_songplays_fact_table >> Load_artist_dim_table
Load_songplays_fact_table >> Load_time_dim_table
Load_song_dim_table >> Run_data_quality_checks
Load_user_dim_table >> Run_data_quality_checks
Load_artist_dim_table >> Run_data_quality_checks
Load_time_dim_table >> Run_data_quality_checks
Run_data_quality_checks >> End_execution
