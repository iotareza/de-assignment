from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import DagRun
from airflow.utils.state import State
from airflow.utils.dates import days_ago
import sys
import os
from airflow.models import Connection
from airflow.hooks.base import BaseHook
from airflow.utils.session import provide_session
from sqlalchemy.orm import Session

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import the data utility functions
from src.data_utils import download_and_upload_movielens_data, verify_s3_data
from src.alerting_service import send_task_failure_alert, send_pipeline_failure_alert

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
    'on_failure_callback': send_task_failure_alert,  # Add failure callback to all tasks
}

# Define the DAG
dag = DAG(
    'movielens_analytics_pipeline',
    default_args=default_args,
    description='Daily pipeline to process MovieLens ml-100k dataset using Spark for analytics. Waits for news sentiment pipeline to complete before running.',
    schedule_interval='0 20 * * 1-5',  # 8 PM Monday-Friday (weekdays only)
    catchup=False,
    tags=['movielens', 'spark', 'analytics', 'recommendations'],
    max_active_runs=1,
    on_failure_callback=send_pipeline_failure_alert,  # Add DAG-level failure callback
)

def create_spark_task(task_id, application_path, memory='1g', cores='1'):
    """
    Helper function to create SparkSubmitOperator with common configuration.
    
    Args:
        task_id (str): The task identifier
        application_path (str): Path to the Spark application
        memory (str): Memory allocation for driver and executor (default: '1g')
        cores (str): Number of cores for executor (default: '1')
    
    Returns:
        SparkSubmitOperator: Configured Spark task
    """
    return SparkSubmitOperator(
        task_id=task_id,
        application=application_path,
        conn_id='spark_default',
        name=task_id,  # Set the application name
        conf={
            'spark.submit.deployMode': 'client',
            'spark.driver.memory': memory,
            'spark.executor.memory': memory,
            'spark.executor.cores': cores,
            'spark.dynamicAllocation.enabled': 'false',
            'spark.jars': '/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.261.jar',
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.access.key': 'minioadmin',
            'spark.hadoop.fs.s3a.secret.key': 'minioadmin',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.hadoop.fs.s3a.connection.maximum': '1000',
            'spark.hadoop.fs.s3a.threads.max': '20',
            # Python version configuration to ensure consistency
            'spark.pyspark.python': 'python3.9',
            'spark.pyspark.driver.python': 'python3.9',
        },
        dag=dag,
    )

@provide_session
def create_spark_connection(session=None, **kwargs):
    conn_id = 'spark_default'
    conn_uri = 'spark://spark-master:7077'
    # Remove if exists
    try:
        BaseHook.get_connection(conn_id)
        kwargs['ti'].log.info(f"Connection {conn_id} already exists, deleting and recreating.")
        session.query(Connection).filter(Connection.conn_id == conn_id).delete()
        session.commit()
    except Exception:
        pass
    # Create new connection
    new_conn = Connection(
        conn_id=conn_id,
        conn_type='spark',
        host=conn_uri,
        extra='{"queue": "default"}'
    )
    session.add(new_conn)
    session.commit()

create_spark_conn_task = PythonOperator(
    task_id='create_spark_connection',
    python_callable=create_spark_connection,
    provide_context=True,
    dag=dag,
)

# Task 1: Check if Spark is available
check_spark_task = BashOperator(
    task_id='check_spark_availability',
    bash_command='curl -f http://spark-master:8080 || exit 1',
    retries=3,
    retry_delay=timedelta(minutes=1),
    dag=dag,
)

def check_news_sentiment_success(**context):
    """
    Check if the news sentiment pipeline completed successfully.
    Looks for the most recent successful run of the news pipeline within 1 hour.
    """
    current_execution_date = context['execution_date']
    
    # Find all news sentiment pipeline runs
    news_runs = DagRun.find(
        dag_id='news_sentiment_pipeline'
    )
    
    if not news_runs:
        raise Exception("No news sentiment pipeline runs found")
    
    # Sort by execution date and find the most recent successful run
    successful_runs = [run for run in news_runs if run.state == State.SUCCESS]
    
    if not successful_runs:
        raise Exception("No successful news sentiment pipeline runs found")
    
    # Get the most recent successful run
    most_recent_run = max(successful_runs, key=lambda x: x.execution_date)
    
    # Check if the most recent run is within 1 hour of current execution time
    time_difference = current_execution_date - most_recent_run.execution_date
    if time_difference > timedelta(hours=1):
        raise Exception(f"Most recent successful news pipeline run at {most_recent_run.execution_date} is too old (more than 1 hour ago). Current time: {current_execution_date}")
    
    context['ti'].log.info(f"Found recent successful news pipeline run at {most_recent_run.execution_date} (within 1 hour)")
    
    # Check if the specific task 'persist_sentiment_data' completed successfully
    persist_task = most_recent_run.get_task_instance('persist_sentiment_data')
    if not persist_task or persist_task.state != State.SUCCESS:
        raise Exception(f"News sentiment pipeline task 'persist_sentiment_data' for {most_recent_run.execution_date} failed or missing")
    
    context['ti'].log.info(f"News sentiment pipeline completed successfully at {most_recent_run.execution_date}")
    return f"News sentiment pipeline success confirmed for {most_recent_run.execution_date}"

# Task 2: Check if news sentiment pipeline completed successfully
check_news_pipeline = PythonOperator(
    task_id='check_news_sentiment_success',
    python_callable=check_news_sentiment_success,
    provide_context=True,
    dag=dag,
)

# Task 3: Download and upload MovieLens data using PythonOperator
download_data_task = PythonOperator(
    task_id='download_and_upload_movielens_data',
    python_callable=download_and_upload_movielens_data,
    dag=dag,
)

# Task 4: Verify raw data uploaded to S3
verify_raw_data_task = PythonOperator(
    task_id='verify_raw_data_s3',
    python_callable=verify_s3_data,
    dag=dag,
)

# Create Spark tasks using the helper function
task1_processing = create_spark_task(
    'task1_mean_age_by_occupation',
    '/opt/airflow/src/spark_apps/task1_mean_age_by_occupation.py'
)

task2_processing = create_spark_task(
    'task2_top_rated_movies',
    '/opt/airflow/src/spark_apps/task2_top_rated_movies.py'
)

task3_processing = create_spark_task(
    'task3_top_genres_by_age_occupation',
    '/opt/airflow/src/spark_apps/task3_top_genres_by_age_occupation.py'
)

task4_processing = create_spark_task(
    'task4_similar_movies',
    '/opt/airflow/src/spark_apps/task4_similar_movies.py',
    memory='2g',
    cores='2'
)

# Define task dependencies
create_spark_conn_task >> check_spark_task >> download_data_task >> verify_raw_data_task >> [task1_processing, task2_processing, task3_processing, task4_processing]

# Add the news pipeline dependency
check_news_pipeline >> create_spark_conn_task