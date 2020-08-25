import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from helpers import SqlQueries

from helpers import create_tables_sql

dag = DAG(
    'Create_Tables',
    start_date=datetime.datetime(2018, 1, 1, 0, 0, 0, 0),
    end_date=datetime.datetime(2019, 1, 1, 0, 0, 0, 0),
    schedule_interval='@monthly',
    max_active_runs=1
)

create_events_table = PostgresOperator(
    task_id="create_events_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql.CREATE_EVENTS_TABLE_SQL
)

create_staging_songs_table = PostgresOperator(
    task_id="create_staging_songs_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql.CREATE_STAGING_SONGS_TABLE_SQL
)

create_songplays_table = PostgresOperator(
    task_id="create_songplays_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql.CREATE_SONGPLAYS_TABLE_SQL
)

create_artists_table = PostgresOperator(
    task_id="create_artists_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql.CREATE_ARTISTS_TABLE_SQL
)

create_songs_table = PostgresOperator(
    task_id="create_songs_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql.CREATE_SONGS_TABLE_SQL
)

create_users_table = PostgresOperator(
    task_id="create_users_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql.CREATE_USERS_TABLE_SQL
)

create_time_table = PostgresOperator(
    task_id="create_time_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql=create_tables_sql.CREATE_TIME_TABLE_SQL
)