from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_extractor import extract_news_data
from src.data_processor import process_news_data
from src.sentiment_analyzer import generate_sentiment_scores
from src.data_persister import persist_sentiment_data
from src.alerting_service import send_task_failure_alert, send_pipeline_failure_alert

# Test function to simulate failure
def test_failure_task(**context):
    """Task that can be made to fail for testing purposes"""
    # You can toggle this to True to make the task fail
    should_fail = context['dag_run'].conf.get('should_fail', False)
    
    if should_fail:
        raise Exception("Simulated failure for testing dependency")
    
    return "Task completed successfully"

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': send_task_failure_alert,  # Add failure callback to all tasks
}

# Define the DAG
dag = DAG(
    'news_sentiment_pipeline',
    default_args=default_args,
    description='Pipeline to fetch news articles and generate sentiment scores for HDFC and Tata Motors using S3 for data sharing',
    schedule_interval='0 19 * * 1-5',  # 7 PM every working day (Monday-Friday)
    catchup=False,
    tags=['news', 'sentiment', 'financial', 's3'],
    on_failure_callback=send_pipeline_failure_alert,  # Add DAG-level failure callback
)

# Task 1: Extract news data and save to S3
extract_task = PythonOperator(
    task_id='extract_news_data',
    python_callable=extract_news_data,
    dag=dag,
)

# Task 2: Process data from S3 and save processed data to S3
process_task = PythonOperator(
    task_id='process_news_data',
    python_callable=process_news_data,
    dag=dag,
)

# Task 3: Generate sentiment scores from S3 and save to S3
sentiment_task = PythonOperator(
    task_id='generate_sentiment_scores',
    python_callable=generate_sentiment_scores,
    dag=dag,
)

# Test task that can be made to fail
# test_failure = PythonOperator(
#     task_id='test_failure',
#     python_callable=test_failure_task,
#     dag=dag,
# )

# Task 4: Persist data from S3 to database
persist_task = PythonOperator(
    task_id='persist_sentiment_data',
    python_callable=persist_sentiment_data,
    dag=dag,
)

# Define task dependencies
extract_task >> process_task >> sentiment_task >> persist_task
# Test task that can be made to fail to verify that the sensor in the movielens pipeline 
# correctly detects failures in this pipeline. This helps ensure proper dependency handling
# between the two pipelines.

# test_failure >> extract_task >> process_task >> sentiment_task  >> persist_task 