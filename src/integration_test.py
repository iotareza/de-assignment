#!/usr/bin/env python3
"""
Integration Test Suite for News Sentiment Pipeline

This script tests the entire pipeline end-to-end:
1. Cleanup test data
2. Extract news data
3. Process news data
4. Generate sentiment scores
5. Persist data to database
6. Display results

All communication between steps is done through S3/MinIO.
"""

import sys
import os
import time
import logging
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from .cleanup_test_data import main as cleanup_test_data
from .data_extractor import extract_news_data
from .data_processor import process_news_data
from .sentiment_analyzer import generate_sentiment_scores
from .data_persister import persist_sentiment_data
from .connection_pool import get_postgres_connection, get_redis_client, get_s3_client
from .s3_storage import S3Storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IntegrationTestSuite:
    def __init__(self):
        self.s3_storage = S3Storage()
        self.test_results = {}
        self.start_time = None
        # Create a single mock task instance to be shared across all tasks
        self.mock_task_instance = MockTaskInstance()
        
    def run_full_pipeline(self):
        """Run the complete integration test pipeline"""
        print("=" * 80)
        print("NEWS SENTIMENT PIPELINE - INTEGRATION TEST SUITE")
        print("=" * 80)
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        self.start_time = time.time()
        
        try:
            # Step 1: Verify services are running
            self._verify_services()
            
            # Step 2: Cleanup test data
            self._run_cleanup()
            
            # Step 3: Extract news data
            self._run_extraction()
            
            # Step 4: Process news data
            self._run_processing()
            
            # Step 5: Generate sentiment scores
            self._run_sentiment_analysis()
            
            # Step 6: Persist data
            self._run_persistence()
            
            # Step 7: Display results
            self._display_results()
            
        except Exception as e:
            logger.error(f"Integration test failed: {str(e)}")
            self._display_error_results(str(e))
            return False
        
        return True
    
    def _verify_services(self):
        """Verify that all required services are running"""
        print("🔍 Verifying services...")
        
        # Test PostgreSQL connection
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
            print("✅ PostgreSQL: Connected")
        except Exception as e:
            raise Exception(f"PostgreSQL connection failed: {str(e)}")
        
        # Test Redis connection
        try:
            redis_conn = get_redis_client()
            redis_conn.ping()
            print("✅ Redis: Connected")
        except Exception as e:
            raise Exception(f"Redis connection failed: {str(e)}")
        
        # Test S3/MinIO connection
        try:
            s3_client = get_s3_client()
            s3_client.list_buckets()
            print("✅ S3/MinIO: Connected")
        except Exception as e:
            raise Exception(f"S3/MinIO connection failed: {str(e)}")
        
        print()
    
    def _run_cleanup(self):
        """Run cleanup to start with a clean slate"""
        print("🧹 Step 1: Cleaning up test data...")
        try:
            cleanup_test_data(close_connections=False)
        except Exception as e:
            raise Exception(f"Cleanup failed: {str(e)}")
        print()
    
    def _run_extraction(self):
        """Run data extraction"""
        print("📰 Step 2: Extracting news data...")
        try:
            # Create a mock context for the extractor
            mock_context = {'task_instance': self.mock_task_instance}
            
            s3_key = extract_news_data(**mock_context)
            self.test_results['extraction'] = {
                's3_key': s3_key,
                'status': 'success'
            }
            
            # Display XCom state after extraction
            self.mock_task_instance.display_xcom_state()
            
            # Load and display extraction results
            data = self.s3_storage.load_data(s3_key)
            article_count = len(data.get('articles', []))
            
            print("✅ Data extraction completed successfully")
            print(f"   - S3 Key: {s3_key}")
            print(f"   - Articles extracted: {article_count}")
            
            # Show article details
            if article_count > 0:
                print("   - Sample articles:")
                for i, article in enumerate(data['articles'][:3]):
                    print(f"     {i+1}. {article.get('title', 'No title')[:60]}...")
                    print(f"        Source: {article.get('source', 'Unknown')}")
                    print(f"        Keyword: {article.get('keyword', 'Unknown')}")
                if article_count > 3:
                    print(f"     ... and {article_count - 3} more articles")
            
        except Exception as e:
            raise Exception(f"Data extraction failed: {str(e)}")
        print()
    
    def _run_processing(self):
        """Run data processing"""
        print("⚙️  Step 3: Processing news data...")
        try:
            # Create a mock context - the extraction task has already pushed data to XCom
            mock_context = {'task_instance': self.mock_task_instance}
            
            s3_key = process_news_data(**mock_context)
            self.test_results['processing'] = {
                's3_key': s3_key,
                'status': 'success'
            }
            
            # Display XCom state after processing
            self.mock_task_instance.display_xcom_state()
            
            # Load and display processing results
            data = self.s3_storage.load_data(s3_key)
            processed_count = data.get('processed_count', 0)
            original_count = data.get('original_count', 0)
            
            print("✅ Data processing completed successfully")
            print(f"   - S3 Key: {s3_key}")
            print(f"   - Original articles: {original_count}")
            print(f"   - Processed articles: {processed_count}")
            print(f"   - Filtered out: {original_count - processed_count}")
            
        except Exception as e:
            raise Exception(f"Data processing failed: {str(e)}")
        print()
    
    def _run_sentiment_analysis(self):
        """Run sentiment analysis"""
        print("😊 Step 4: Generating sentiment scores...")
        try:
            # Create a mock context - the processing task has already pushed data to XCom
            mock_context = {'task_instance': self.mock_task_instance}
            
            s3_key = generate_sentiment_scores(**mock_context)
            self.test_results['sentiment'] = {
                's3_key': s3_key,
                'status': 'success'
            }
            
            # Display XCom state after sentiment analysis
            self.mock_task_instance.display_xcom_state()
            
            # Load and display sentiment results
            data = self.s3_storage.load_data(s3_key)
            articles_analyzed = data.get('articles_analyzed', 0)
            aggregate_scores = data.get('aggregate_scores', {})
            
            print("✅ Sentiment analysis completed successfully")
            print(f"   - S3 Key: {s3_key}")
            print(f"   - Articles analyzed: {articles_analyzed}")
            
            # Show aggregate scores
            if aggregate_scores:
                print("   - Aggregate sentiment scores:")
                for keyword, scores in aggregate_scores.items():
                    print(f"     {keyword}:")
                    print(f"       Average: {scores.get('average_score', 0):.3f}")
                    print(f"       Min: {scores.get('min_score', 0):.3f}")
                    print(f"       Max: {scores.get('max_score', 0):.3f}")
                    print(f"       Articles: {scores.get('article_count', 0)}")
            
        except Exception as e:
            raise Exception(f"Sentiment analysis failed: {str(e)}")
        print()
    
    def _run_persistence(self):
        """Run data persistence"""
        print("💾 Step 5: Persisting data to database...")
        try:
            # Create a mock context - the sentiment analysis task has already pushed data to XCom
            mock_context = {'task_instance': self.mock_task_instance}
            
            result = persist_sentiment_data(**mock_context)
            self.test_results['persistence'] = result
            
            # Display XCom state after persistence
            self.mock_task_instance.display_xcom_state()
            
            print("✅ Data persistence completed successfully")
            print(f"   - Individual records: {result.get('individual_records_persisted', 0)}")
            print(f"   - Aggregate records: {result.get('aggregate_records_persisted', 0)}")
            print(f"   - Total records: {result.get('total_records_persisted', 0)}")
            print(f"   - Pipeline run ID: {result.get('pipeline_run_id', 'N/A')}")
            
        except Exception as e:
            raise Exception(f"Data persistence failed: {str(e)}")
        print()
    
    def _display_results(self):
        """Display comprehensive test results"""
        print("=" * 80)
        print("INTEGRATION TEST RESULTS")
        print("=" * 80)
        
        # Calculate total time
        total_time = time.time() - self.start_time
        
        # Display summary
        print(f"✅ All pipeline steps completed successfully!")
        print(f"⏱️  Total execution time: {total_time:.2f} seconds")
        print()
        
        # Display step-by-step results
        print("📊 Step-by-step Results:")
        print(f"   1. Cleanup: ✅ {self.test_results.get('cleanup', {}).get('status', 'unknown')}")
        print(f"   2. Extraction: ✅ {self.test_results.get('extraction', {}).get('status', 'unknown')}")
        print(f"   3. Processing: ✅ {self.test_results.get('processing', {}).get('status', 'unknown')}")
        print(f"   4. Sentiment Analysis: ✅ {self.test_results.get('sentiment', {}).get('status', 'unknown')}")
        print(f"   5. Persistence: ✅ {self.test_results.get('persistence', {}).get('status', 'unknown')}")
        print()
        
        # Display final data summary
        persistence_result = self.test_results.get('persistence', {})
        summary_report = persistence_result.get('summary_report', {})
        
        print("📈 Final Data Summary:")
        print(f"   - Total articles in database: {summary_report.get('total_articles', 0)}")
        print(f"   - Total sentiment scores: {summary_report.get('total_sentiment_scores', 0)}")
        print(f"   - Total aggregate scores: {summary_report.get('total_aggregate_scores', 0)}")
        print()
        
        # Display latest sentiment scores
        latest_scores = summary_report.get('latest_scores', [])
        if latest_scores:
            print("🎯 Latest Sentiment Scores:")
            for score in latest_scores:
                print(f"   - {score['keyword']}: {score['average_score']:.3f} ({score['article_count']} articles)")
            print()
        
        # Display keyword distribution
        keyword_dist = summary_report.get('keyword_distribution', [])
        if keyword_dist:
            print("📊 Keyword Distribution:")
            for item in keyword_dist:
                print(f"   - {item['keyword']}: {item['count']} articles")
            print()
        
        # Display source distribution
        source_dist = summary_report.get('source_distribution', [])
        if source_dist:
            print("📰 Source Distribution:")
            for item in source_dist:
                print(f"   - {item['source']}: {item['count']} articles")
            print()
        
        print("🎉 Integration test completed successfully!")
        print("=" * 80)
    
    def _display_error_results(self, error_message):
        """Display error results when test fails"""
        print("=" * 80)
        print("❌ INTEGRATION TEST FAILED")
        print("=" * 80)
        print(f"Error: {error_message}")
        print()
        print("🔍 Troubleshooting tips:")
        print("   1. Ensure PostgreSQL, Redis, and MinIO are running")
        print("   2. Check connection parameters in config.py")
        print("   3. Verify network connectivity to all services")
        print("   4. Check service logs for detailed error messages")
        print("=" * 80)


class MockTaskInstance:
    """Mock Airflow task instance for testing"""
    def __init__(self):
        self.xcom_data = {}
    
    def xcom_pull(self, task_ids, key):
        """Mock XCom pull method"""
        value = self.xcom_data.get(key)
        print(f"🔍 XCom PULL: task_ids='{task_ids}', key='{key}' -> value={value}")
        return value
    
    def xcom_push(self, key, value):
        """Mock XCom push method"""
        print(f"📤 XCom PUSH: key='{key}', value='{value}'")
        self.xcom_data[key] = value
    
    def display_xcom_state(self):
        """Display current XCom state for debugging"""
        print("📊 Current XCom State:")
        if not self.xcom_data:
            print("   (empty)")
        else:
            for key, value in self.xcom_data.items():
                print(f"   {key}: {value}")
        print()


def main():
    """Main function to run the integration test"""
    try:
        test_suite = IntegrationTestSuite()
        success = test_suite.run_full_pipeline()
        
        if success:
            print("\n🎉 Integration test completed successfully!")
            return 0
        else:
            print("\n❌ Integration test failed!")
            return 1
            
    except KeyboardInterrupt:
        print("\n⚠️  Integration test interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main()) 