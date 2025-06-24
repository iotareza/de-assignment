#!/usr/bin/env python3
"""
Test script to demonstrate the alerting system functionality.
This script simulates various pipeline failures and logs alerts locally.
"""

import time
import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.alerting_service import send_custom_alert, alert_service

def test_alerting_system():
    """
    Test the alerting system with various scenarios.
    """
    print("🧪 Testing Alerting System (Local Logging)")
    print("="*60)
    
    # Test 1: Task failure alert
    print("\n1. Testing task failure alert...")
    success = send_custom_alert(
        pipeline_name="news_sentiment_pipeline",
        task_name="extract_news_data",
        message="Failed to connect to news API: Connection timeout",
        severity="HIGH"
    )
    print(f"   Result: {'✅ Success' if success else '❌ Failed'}")
    
    time.sleep(1)
    
    # Test 2: Pipeline failure alert
    print("\n2. Testing pipeline failure alert...")
    success = alert_service.send_pipeline_failure_alert(
        pipeline_name="movielens_analytics_pipeline",
        error_message="Multiple Spark tasks failed due to insufficient memory",
        failed_tasks=["task1_processing", "task2_processing", "task3_processing"]
    )
    print(f"   Result: {'✅ Success' if success else '❌ Failed'}")
    
    time.sleep(1)
    
    # Test 3: Critical alert
    print("\n3. Testing critical alert...")
    success = send_custom_alert(
        pipeline_name="news_sentiment_pipeline",
        task_name="persist_sentiment_data",
        message="Database connection lost - data may be corrupted",
        severity="CRITICAL"
    )
    print(f"   Result: {'✅ Success' if success else '❌ Failed'}")
    
    time.sleep(1)
    
    # Test 4: Low severity alert
    print("\n4. Testing low severity alert...")
    success = send_custom_alert(
        pipeline_name="movielens_analytics_pipeline",
        task_name="verify_raw_data_s3",
        message="S3 data verification took longer than expected (5 minutes)",
        severity="LOW"
    )
    print(f"   Result: {'✅ Success' if success else '❌ Failed'}")
    
    print("\n" + "="*60)
    print("✅ Alerting system test completed!")
    print("\n📊 All alerts have been logged to console and log files")

def test_with_context():
    """
    Test alerting with additional context information.
    """
    print("\n🧪 Testing Alerting with Context")
    print("="*60)
    
    additional_context = {
        "execution_date": "2024-01-15T20:00:00Z",
        "dag_run_id": "manual__2024-01-15T20:00:00+00:00",
        "failed_tasks": ["extract_news_data", "process_news_data"],
        "retry_count": 2,
        "max_retries": 3,
        "environment": "production",
        "alert_type": "task_failure"
    }
    
    success = alert_service.send_alert(
        pipeline_name="news_sentiment_pipeline",
        task_name="extract_news_data",
        error_message="News API rate limit exceeded - 429 Too Many Requests",
        severity="HIGH",
        additional_context=additional_context
    )
    
    print(f"   Result: {'✅ Success' if success else '❌ Failed'}")
    print("   Context included: execution_date, dag_run_id, failed_tasks, retry_count, etc.")

def test_all_severity_levels():
    """
    Test all severity levels to show different formatting.
    """
    print("\n🧪 Testing All Severity Levels")
    print("="*60)
    
    severities = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    test_messages = [
        "Minor warning: Task took longer than usual",
        "Moderate issue: Some data validation failed",
        "High priority: Critical component unavailable",
        "Critical failure: System completely down"
    ]
    
    for i, (severity, message) in enumerate(zip(severities, test_messages), 1):
        print(f"\n{i}. Testing {severity} severity...")
        success = send_custom_alert(
            pipeline_name="test_pipeline",
            task_name=f"test_task_{i}",
            message=message,
            severity=severity
        )
        print(f"   Result: {'✅ Success' if success else '❌ Failed'}")
        time.sleep(0.5)

if __name__ == "__main__":
    print("🚀 Starting Alerting System Test (Local Logging)")
    print("This test will demonstrate local alert logging without any external services")
    print()
    
    try:
        test_alerting_system()
        test_with_context()
        test_all_severity_levels()
        
        print("\n🎉 All tests completed successfully!")
        print("\n💡 Alert System Features:")
        print("   ✅ Local logging with beautiful formatting")
        print("   ✅ Different severity levels with color coding")
        print("   ✅ Rich context information")
        print("   ✅ Automatic integration with Airflow pipelines")
        print("   ✅ No external dependencies required")
        
        print("\n📋 When pipelines fail, you'll see alerts like:")
        print("   🔴 PIPELINE ALERT: news_sentiment_pipeline - extract_news_data")
        print("   🚨 Severity: HIGH")
        print("   📋 Error: Connection timeout")
        print("   📊 Context: {execution_date, dag_run_id, etc.}")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc() 