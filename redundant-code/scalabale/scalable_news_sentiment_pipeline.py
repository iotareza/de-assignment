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

from src.config import get_stock_source_combinations, STOCKS, get_enabled_sources
from src.scalable_data_extractor import extract_stock_source_data
from src.scalable_data_processor import process_stock_data
from src.scalable_sentiment_analyzer import analyze_stock_sentiment
from src.scalable_data_persister import persist_stock_data

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
    'scalable_news_sentiment_pipeline',
    default_args=default_args,
    description='Scalable pipeline to fetch news articles and generate sentiment scores for multiple stocks',
    schedule_interval='0 19 * * 1-5',  # 7 PM every working day (Monday-Friday)
    catchup=False,
    tags=['news', 'sentiment', 'financial', 'scalable'],
    max_active_runs=1,
)

def create_extraction_tasks():
    """Create dynamic extraction tasks for each stock-source combination"""
    tasks = {}
    combinations = get_stock_source_combinations()
    
    for stock_name, source_name in combinations:
        task_id = f'extract_{stock_name.lower().replace(" ", "_")}_{source_name.lower()}'
        
        task = PythonOperator(
            task_id=task_id,
            python_callable=extract_stock_source_data,
            op_kwargs={
                'stock_name': stock_name,
                'source_name': source_name
            },
            dag=dag,
            pool='extraction_pool',  # Limit concurrent extractions
            pool_slots=1,
        )
        tasks[f"{stock_name}_{source_name}"] = task
    
    return tasks

def create_processing_tasks():
    """Create dynamic processing tasks for each stock"""
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
    """Create dynamic sentiment analysis tasks for each stock"""
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
    """Create dynamic persistence tasks for each stock"""
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

# Create all dynamic tasks
extraction_tasks = create_extraction_tasks()
processing_tasks = create_processing_tasks()
sentiment_tasks = create_sentiment_tasks()
persistence_tasks = create_persistence_tasks()

# Create aggregation tasks
def aggregate_results(**context):
    """Aggregate results from all stocks"""
    from src.scalable_aggregator import aggregate_all_results
    return aggregate_all_results()

aggregate_task = PythonOperator(
    task_id='aggregate_results',
    python_callable=aggregate_results,
    dag=dag,
    trigger_rule='all_done',  # Run even if some tasks fail
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

# Define task dependencies
# 1. All extraction tasks can run in parallel
# 2. Processing tasks depend on their respective extraction tasks
# 3. Sentiment tasks depend on their respective processing tasks
# 4. Persistence tasks depend on their respective sentiment tasks
# 5. Aggregate task depends on all persistence tasks
# 6. Notification task depends on aggregate task

# Set up dependencies for each stock
for stock in STOCKS:
    stock_name = stock['name']
    
    # Get all extraction tasks for this stock
    stock_extraction_tasks = [
        task for task_name, task in extraction_tasks.items() 
        if task_name.startswith(f"{stock_name}_")
    ]
    
    # Set up the pipeline for this stock
    if stock_extraction_tasks:
        # All extraction tasks for this stock -> processing task
        processing_tasks[stock_name].set_upstream(stock_extraction_tasks)
        
        # Processing task -> sentiment task
        sentiment_tasks[stock_name].set_upstream(processing_tasks[stock_name])
        
        # Sentiment task -> persistence task
        persistence_tasks[stock_name].set_upstream(sentiment_tasks[stock_name])

# All persistence tasks -> aggregate task
aggregate_task.set_upstream(list(persistence_tasks.values()))

# Aggregate task -> notification task
notification_task.set_upstream(aggregate_task) 