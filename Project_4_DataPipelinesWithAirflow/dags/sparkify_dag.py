from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner':'udacity',
    'start_date':datetime(2021,06,26),
    'depends_on_past':False,
    'retries':1,
    'retry_delay':timedelta(seconds=500),
    'catchup':False
    } 
    
with DAG(
    'sparkify_dag',
    default_args=default_args,
    description='Load and transform Data in Redschift using Airflow',
    schedule_interval='0 * * * *'
    ):
    
    start_operator = DummyOperator(task_id='Begin_execution')
    
    stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json"
    )
    
    stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data"
    )
    
    load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    table="songplays",
    select_query=SqlQueries.songplay_table_insert
    )
    
    load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    table="users",
    truncate_table=True,
    select_query=SqlQueries.user_table_insert
    )
    
    load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    table="songs",
    truncate_table=True,
    select_query=SqlQueries.song_table_insert
    )
    
    load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    table="artists",
    truncate_table=True,
    select_query=SqlQueries.artist_table_insert
    )
    
    load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    table="time",
    truncate_table=True,
    select_query=SqlQueries.time_table_insert
    )
    
    run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    tables=[
        "songplays",
        "users",
        "songs",
        "artists",
        "time"
    ],
    )
    
    end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
    
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
    [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator