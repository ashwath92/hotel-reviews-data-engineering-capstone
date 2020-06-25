from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                                CopyDimensionOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'ashwath',
    'depends_on_past': False,
    'start_date': datetime(2017, 11, 1),
    'end_date': datetime(2017, 11, 2),
    'email': ['ashwath92@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    #'retries': 3,
    #'retry_delay': timedelta(minutes=5),
    'catchup': False
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# hourly: cron is '0 * * * *': https://airflow.apache.org/docs/stable/scheduler.html
dag = DAG('sparkify_elt_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_reviews',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_hotels',
    dag=dag
)

load_reviews_table = LoadFactOperator(
    task_id='Load_reviews_fact_table',
    dag=dag
)

load_hotels_dimension_table = LoadDimensionOperator(
    task_id='Load_hotels_dim_table',
    dag=dag
)

load_hoteladdresses_dimension_table = LoadDimensionOperator(
    task_id='Load_hoteladdresses_dim_table',
    dag=dag
)

load_hotelreviewmetadata_dimension_table = LoadDimensionOperator(
    task_id='Load_hotelreviewmetadata_dim_table',
    dag=dag
)

load_date_dimension_table = LoadDimensionOperator(
    task_id='Load_date_dim_table',
    dag=dag
)

load_date_dimension_table = LoadDimensionOperator(
    task_id='Load_date_dim_table',
    dag=dag
)

load_airports_dimension_table = CopyDimensionOperator(
    task_id='Load_airports_dim_table',
    dag=dag
)

load_countryindicators_dimension_table = CopyDimensionOperator(
    task_id='Load_countryindicators_dim_table',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)