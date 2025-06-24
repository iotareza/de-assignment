import logging
from datetime import datetime
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlertingService:
    """
    Simple alerting service for pipeline failure notifications.
    Logs alerts locally with detailed formatting.
    """
    
    def __init__(self):
        self.service_name = "Pipeline Alerting Service"
    
    def send_alert(self, 
                   pipeline_name: str, 
                   task_name: str, 
                   error_message: str, 
                   severity: str = "HIGH",
                   additional_context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Log an alert for pipeline/task failure.
        
        Args:
            pipeline_name: Name of the pipeline (DAG)
            task_name: Name of the failed task
            error_message: Error message or description
            severity: Alert severity (LOW, MEDIUM, HIGH, CRITICAL)
            additional_context: Additional context information
            
        Returns:
            bool: Always returns True since we're just logging
        """
        try:
            alert_payload = {
                "timestamp": datetime.now().isoformat(),
                "pipeline_name": pipeline_name,
                "task_name": task_name,
                "error_message": error_message,
                "severity": severity,
                "service": self.service_name,
                "additional_context": additional_context or {}
            }
            
            # Log the alert with detailed formatting
            self._log_alert(alert_payload)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to log alert: {str(e)}")
            return False
    
    def _log_alert(self, alert_payload: Dict[str, Any]):
        """
        Log alert with beautiful formatting.
        """
        severity_emoji = {
            "LOW": "🔵",
            "MEDIUM": "🟡", 
            "HIGH": "🟠",
            "CRITICAL": "🔴"
        }
        
        emoji = severity_emoji.get(alert_payload['severity'], "⚪")
        
        # Print formatted alert to console
        print("\n" + "="*80)
        print(f"{emoji} PIPELINE ALERT: {alert_payload['pipeline_name']} - {alert_payload['task_name']}")
        print("="*80)
        print(f"🚨 Severity: {alert_payload['severity']}")
        print(f"⏰ Timestamp: {alert_payload['timestamp']}")
        print(f"📋 Error: {alert_payload['error_message']}")
        
        if alert_payload.get('additional_context'):
            print(f"📊 Context: {alert_payload['additional_context']}")
        
        print("="*80 + "\n")
        
        # Also log to logger for file logging
        logger.error(f"PIPELINE ALERT - {alert_payload['pipeline_name']} - {alert_payload['task_name']} - {alert_payload['severity']} - {alert_payload['error_message']}")
    
    def send_pipeline_failure_alert(self, 
                                   pipeline_name: str, 
                                   error_message: str,
                                   failed_tasks: Optional[list] = None) -> bool:
        """
        Log an alert for entire pipeline failure.
        
        Args:
            pipeline_name: Name of the failed pipeline
            error_message: Error message or description
            failed_tasks: List of failed task names
            
        Returns:
            bool: Always returns True since we're just logging
        """
        additional_context = {
            "failed_tasks": failed_tasks or [],
            "alert_type": "pipeline_failure"
        }
        
        return self.send_alert(
            pipeline_name=pipeline_name,
            task_name="PIPELINE_FAILURE",
            error_message=error_message,
            severity="CRITICAL",
            additional_context=additional_context
        )

# Global alerting service instance
alert_service = AlertingService()

def send_task_failure_alert(context):
    """
    Callback function to log alerts when a task fails.
    This function can be used as on_failure_callback in Airflow tasks.
    
    Args:
        context: Airflow context dictionary
    """
    try:
        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        execution_date = context['execution_date']
        exception = context.get('exception', 'Unknown error')
        
        additional_context = {
            "execution_date": execution_date.isoformat(),
            "dag_run_id": context['dag_run'].run_id,
            "task_instance_key": context['task_instance_key'],
            "alert_type": "task_failure"
        }
        
        success = alert_service.send_alert(
            pipeline_name=dag_id,
            task_name=task_id,
            error_message=str(exception),
            severity="HIGH",
            additional_context=additional_context
        )
        
        if success:
            logger.info(f"Alert logged successfully for failed task: {dag_id}.{task_id}")
        else:
            logger.error(f"Failed to log alert for task: {dag_id}.{task_id}")
            
    except Exception as e:
        logger.error(f"Error in send_task_failure_alert: {str(e)}")

def send_pipeline_failure_alert(context):
    """
    Callback function to log alerts when an entire pipeline fails.
    This function can be used as on_failure_callback in Airflow DAGs.
    
    Args:
        context: Airflow context dictionary
    """
    try:
        dag_id = context['dag'].dag_id
        execution_date = context['execution_date']
        exception = context.get('exception', 'Unknown pipeline error')
        
        # Get failed tasks
        failed_tasks = []
        if 'task_instances' in context:
            for task_instance in context['task_instances']:
                if task_instance.state == 'failed':
                    failed_tasks.append(task_instance.task_id)
        
        additional_context = {
            "execution_date": execution_date.isoformat(),
            "dag_run_id": context['dag_run'].run_id,
            "failed_tasks": failed_tasks,
            "alert_type": "pipeline_failure"
        }
        
        success = alert_service.send_pipeline_failure_alert(
            pipeline_name=dag_id,
            error_message=str(exception),
            failed_tasks=failed_tasks
        )
        
        if success:
            logger.info(f"Pipeline failure alert logged successfully for: {dag_id}")
        else:
            logger.error(f"Failed to log pipeline failure alert for: {dag_id}")
            
    except Exception as e:
        logger.error(f"Error in send_pipeline_failure_alert: {str(e)}")

def send_custom_alert(pipeline_name: str, 
                     task_name: str, 
                     message: str, 
                     severity: str = "MEDIUM") -> bool:
    """
    Send a custom alert for any pipeline event.
    
    Args:
        pipeline_name: Name of the pipeline
        task_name: Name of the task or event
        message: Alert message
        severity: Alert severity
        
    Returns:
        bool: True if alert was logged successfully, False otherwise
    """
    return alert_service.send_alert(
        pipeline_name=pipeline_name,
        task_name=task_name,
        error_message=message,
        severity=severity
    ) 