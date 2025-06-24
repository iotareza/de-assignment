from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import sys
import os
from typing import List, Dict, Any

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import STOCKS
from src.scalable.url_collector import collect_stock_urls
from src.scalable.pre_extractor_filter import filter_stock_urls
from src.scalable.content_extractor import extract_stock_content
from src.scalable.scalable_data_processor import process_stock_data
from src.scalable.scalable_sentiment_analyzer import analyze_stock_sentiment
from src.scalable.scalable_data_persister import persist_stock_data

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
}

# Define the DAG
dag = DAG(
    'three_step_extraction_pipeline',
    default_args=default_args,
    description='Three-step pipeline: URL Collection -> PreExtractor Filter -> Content Extraction',
    schedule_interval='0 19 * * 1-5',  # 7 PM every working day (Monday-Friday)
    catchup=False,
    tags=['news', 'sentiment', 'financial', 'three-step'],
    max_active_runs=1,
)

def create_url_collection_tasks():
    """Create URL collection tasks for each stock"""
    tasks = {}
    
    for stock in STOCKS:
        stock_name = stock['name']
        task_id = f'collect_urls_{stock_name.lower().replace(" ", "_")}'
        
        task = PythonOperator(
            task_id=task_id,
            python_callable=collect_stock_urls,
            op_kwargs={'stock_name': stock_name},
            dag=dag,
            pool='url_collection_pool',
            pool_slots=1,
        )
        tasks[stock_name] = task
    
    return tasks

def create_pre_extractor_filter_tasks():
    """Create preExtractor filter tasks for each stock"""
    tasks = {}
    
    for stock in STOCKS:
        stock_name = stock['name']
        task_id = f'filter_urls_{stock_name.lower().replace(" ", "_")}'
        
        task = PythonOperator(
            task_id=task_id,
            python_callable=filter_stock_urls,
            op_kwargs={'stock_name': stock_name},
            dag=dag,
            pool='filter_pool',
            pool_slots=1,
        )
        tasks[stock_name] = task
    
    return tasks

def create_content_extraction_tasks():
    """Create content extraction tasks for each stock"""
    tasks = {}
    
    for stock in STOCKS:
        stock_name = stock['name']
        task_id = f'extract_content_{stock_name.lower().replace(" ", "_")}'
        
        task = PythonOperator(
            task_id=task_id,
            python_callable=extract_stock_content,
            op_kwargs={'stock_name': stock_name},
            dag=dag,
            pool='extraction_pool',
            pool_slots=1,
        )
        tasks[stock_name] = task
    
    return tasks

def create_processing_tasks():
    """Create processing tasks for each stock"""
    tasks = {}
    
    for stock in STOCKS:
        stock_name = stock['name']
        task_id = f'process_{stock_name.lower().replace(" ", "_")}'
        
        task = PythonOperator(
            task_id=task_id,
            python_callable=process_stock_data,
            op_kwargs={'stock_name': stock_name},
            dag=dag,
            pool='processing_pool',
            pool_slots=1,
        )
        tasks[stock_name] = task
    
    return tasks

def create_sentiment_tasks():
    """Create sentiment analysis tasks for each stock"""
    tasks = {}
    
    for stock in STOCKS:
        stock_name = stock['name']
        task_id = f'sentiment_{stock_name.lower().replace(" ", "_")}'
        
        task = PythonOperator(
            task_id=task_id,
            python_callable=analyze_stock_sentiment,
            op_kwargs={'stock_name': stock_name},
            dag=dag,
            pool='sentiment_pool',
            pool_slots=1,
        )
        tasks[stock_name] = task
    
    return tasks

def create_persistence_tasks():
    """Create persistence tasks for each stock"""
    tasks = {}
    
    for stock in STOCKS:
        stock_name = stock['name']
        task_id = f'persist_{stock_name.lower().replace(" ", "_")}'
        
        task = PythonOperator(
            task_id=task_id,
            python_callable=persist_stock_data,
            op_kwargs={'stock_name': stock_name},
            dag=dag,
            pool='persistence_pool',
            pool_slots=1,
        )
        tasks[stock_name] = task
    
    return tasks

# Create all task groups
url_collection_tasks = create_url_collection_tasks()
pre_extractor_filter_tasks = create_pre_extractor_filter_tasks()
content_extraction_tasks = create_content_extraction_tasks()
processing_tasks = create_processing_tasks()
sentiment_tasks = create_sentiment_tasks()
persistence_tasks = create_persistence_tasks()

# Create aggregation task
def aggregate_results(**context):
    """Aggregate results from all stocks"""
    from src.scalable_aggregator import aggregate_all_results
    return aggregate_all_results()

aggregate_task = PythonOperator(
    task_id='aggregate_results',
    python_callable=aggregate_results,
    dag=dag,
    trigger_rule='all_done',
)

# Create notification task
def send_completion_notification(**context):
    """Send notification when pipeline completes"""
    from src.notification_service import send_pipeline_completion_notification
    
    # Get task instance info
    ti = context['task_instance']
    dag_run = context['dag_run']
    
    # Count successful and failed tasks
    successful_tasks = len([task for task in dag_run.get_task_instances() if task.state == 'success'])
    failed_tasks = len([task for task in dag_run.get_task_instances() if task.state == 'failed'])
    
    send_pipeline_completion_notification(
        dag_run_id=dag_run.run_id,
        successful_tasks=successful_tasks,
        failed_tasks=failed_tasks,
        total_tasks=len(dag_run.get_task_instances())
    )

notification_task = PythonOperator(
    task_id='send_completion_notification',
    python_callable=send_completion_notification,
    dag=dag,
    trigger_rule='all_done',
)

# Define task dependencies for each stock
for stock in STOCKS:
    stock_name = stock['name']
    
    # Step 1: URL Collection -> Step 2: PreExtractor Filter
    pre_extractor_filter_tasks[stock_name].set_upstream(url_collection_tasks[stock_name])
    
    # Step 2: PreExtractor Filter -> Step 3: Content Extraction
    content_extraction_tasks[stock_name].set_upstream(pre_extractor_filter_tasks[stock_name])
    
    # Step 3: Content Extraction -> Processing
    processing_tasks[stock_name].set_upstream(content_extraction_tasks[stock_name])
    
    # Processing -> Sentiment Analysis
    sentiment_tasks[stock_name].set_upstream(processing_tasks[stock_name])
    
    # Sentiment Analysis -> Persistence
    persistence_tasks[stock_name].set_upstream(sentiment_tasks[stock_name])

# All persistence tasks -> Aggregate task
aggregate_task.set_upstream(list(persistence_tasks.values()))

# Aggregate task -> Notification task
notification_task.set_upstream(aggregate_task) 