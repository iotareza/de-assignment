#!/usr/bin/env python3
"""
Script to preview output data from News Sentiment Pipeline
This script reads and displays the results from database and S3 storage
Uses the existing connection_pool.py for consistent connection management
"""

import os
import sys
import logging
import pandas as pd
import json
from datetime import datetime
import io

# Import the existing connection pool
from .connection_pool import get_postgres_connection, get_s3_client, test_connections

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NewsSentimentOutputPreview:
    """Class to preview News Sentiment pipeline output data using pandas"""
    
    def __init__(self):
        """Initialize the preview class using existing connection pool"""
        logger.info("Initialized News Sentiment Output Preview using connection pool")
    
    def check_connections(self):
        """Test all connections using the connection pool"""
        try:
            connection_status = test_connections()
            logger.info("Connection status:")
            for service, status in connection_status.items():
                status_str = "✓ CONNECTED" if status else "✗ DISCONNECTED"
                logger.info(f"  {service.capitalize()}: {status_str}")
            return connection_status
        except Exception as e:
            logger.error(f"Failed to test connections: {e}")
            return {'postgres': False, 'redis': False, 's3': False}
    
    def list_s3_buckets(self):
        """List available S3 buckets"""
        try:
            s3_client = get_s3_client()
            if not s3_client:
                logger.error("S3 client not available")
                return []
            
            if hasattr(s3_client, 'list_buckets'):  # MinIO
                buckets = s3_client.list_buckets()
                return [bucket.name for bucket in buckets]
            else:  # AWS S3
                response = s3_client.list_buckets()
                return [bucket['Name'] for bucket in response.get('Buckets', [])]
        except Exception as e:
            logger.error(f"Failed to list S3 buckets: {e}")
            return []

    def list_s3_objects(self, bucket_name=None, prefix=''):
        """List objects in S3 bucket with given prefix using connection pool"""
        try:
            s3_client = get_s3_client()
            if not s3_client:
                logger.error("S3 client not available")
                return []
            
            # If no bucket specified, try to find the right one
            if not bucket_name:
                buckets = self.list_s3_buckets()
                if not buckets:
                    logger.error("No S3 buckets available")
                    return []
                
                # Try to find news sentiment bucket
                for bucket in buckets:
                    if 'news' in bucket.lower() or 'sentiment' in bucket.lower():
                        bucket_name = bucket
                        break
                
                if not bucket_name:
                    bucket_name = buckets[0]  # Use first available bucket
            
            if hasattr(s3_client, 'list_objects'):  # MinIO
                objects = s3_client.list_objects(bucket_name, prefix=prefix, recursive=True)
                return [obj.object_name for obj in objects]
            else:  # AWS S3
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix
                )
                objects = response.get('Contents', [])
                return [obj['Key'] for obj in objects]
        except Exception as e:
            logger.error(f"Failed to list S3 objects in bucket {bucket_name}: {e}")
            return []
    
    def read_json_from_s3(self, key, bucket_name=None):
        """Read JSON file from S3/MinIO using connection pool"""
        try:
            logger.info(f"Reading JSON file: {key}")
            s3_client = get_s3_client()
            if not s3_client:
                logger.error("S3 client not available")
                return None
            
            # If no bucket specified, try to find the right one
            if not bucket_name:
                buckets = self.list_s3_buckets()
                if not buckets:
                    logger.error("No S3 buckets available")
                    return None
                
                # Try to find news sentiment bucket
                for bucket in buckets:
                    if 'news' in bucket.lower() or 'sentiment' in bucket.lower():
                        bucket_name = bucket
                        break
                
                if not bucket_name:
                    bucket_name = buckets[0]  # Use first available bucket
            
            if hasattr(s3_client, 'get_object'):  # MinIO
                response = s3_client.get_object(bucket_name, key)
                data = json.loads(response.read().decode('utf-8'))
            else:  # AWS S3
                response = s3_client.get_object(Bucket=bucket_name, Key=key)
                data = json.loads(response['Body'].read().decode('utf-8'))
            
            logger.info(f"Successfully read JSON data from {key}")
            return data
        except Exception as e:
            logger.error(f"Failed to read {key}: {e}")
            return None
    
    def query_database(self, query, params=None):
        """Execute a database query using connection pool and return results as pandas DataFrame"""
        try:
            with get_postgres_connection() as conn:
                # Use cursor to execute query and fetch results
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    
                    # Convert to pandas DataFrame
                    if rows:
                        df = pd.DataFrame(rows, columns=columns)
                    else:
                        df = pd.DataFrame(columns=columns)
                    
                    return df
        except Exception as e:
            logger.error(f"Database query failed: {e}")
            return None
    
    def preview_database_data(self):
        """Preview data from database tables"""
        logger.info("\n" + "="*60)
        logger.info("DATABASE DATA PREVIEW")
        logger.info("="*60)
        
        # Preview Articles table
        logger.info("\n📰 ARTICLES TABLE (2-3 records):")
        articles_df = self.query_database("""
            SELECT article_hash, keyword, source, title, url, content_length, 
                   extraction_timestamp, created_at
            FROM articles 
            ORDER BY created_at DESC 
            LIMIT 3
        """)
        
        if articles_df is not None and not articles_df.empty:
            logger.info(f"Found {len(articles_df)} articles")
            logger.info(articles_df.to_string(index=False))
        else:
            logger.info("No articles found in database")
        
        # Preview Sentiment Scores table
        logger.info("\n😊 SENTIMENT SCORES TABLE (2-3 records):")
        sentiment_df = self.query_database("""
            SELECT article_hash, keyword, source, mock_sentiment_score, 
                   textblob_polarity, textblob_subjectivity, analysis_timestamp
            FROM sentiment_scores 
            ORDER BY analysis_timestamp DESC 
            LIMIT 3
        """)
        
        if sentiment_df is not None and not sentiment_df.empty:
            logger.info(f"Found {len(sentiment_df)} sentiment scores")
            logger.info(sentiment_df.to_string(index=False))
        else:
            logger.info("No sentiment scores found in database")
        
        # Preview Aggregate Scores table
        logger.info("\n📊 AGGREGATE SCORES TABLE (2-3 records):")
        aggregate_df = self.query_database("""
            SELECT keyword, date, average_score, min_score, max_score, 
                   article_count, created_at
            FROM aggregate_scores 
            ORDER BY date DESC, created_at DESC 
            LIMIT 3
        """)
        
        if aggregate_df is not None and not aggregate_df.empty:
            logger.info(f"Found {len(aggregate_df)} aggregate scores")
            logger.info(aggregate_df.to_string(index=False))
        else:
            logger.info("No aggregate scores found in database")
        
        # Preview Pipeline Runs table
        logger.info("\n🔄 PIPELINE RUNS TABLE (2-3 records):")
        pipeline_df = self.query_database("""
            SELECT run_id, start_time, end_time, status, articles_extracted, 
                   articles_processed, articles_analyzed, created_at
            FROM pipeline_runs 
            ORDER BY start_time DESC 
            LIMIT 3
        """)
        
        if pipeline_df is not None and not pipeline_df.empty:
            logger.info(f"Found {len(pipeline_df)} pipeline runs")
            logger.info(pipeline_df.to_string(index=False))
        else:
            logger.info("No pipeline runs found in database")
    
    def preview_s3_data(self):
        """Preview data from S3 storage"""
        logger.info("\n" + "="*60)
        logger.info("S3 STORAGE DATA PREVIEW")
        logger.info("="*60)
        
        # List available buckets
        buckets = self.list_s3_buckets()
        logger.info(f"Available S3 buckets: {buckets}")
        
        if not buckets:
            logger.warning("No S3 buckets found")
            return
        
        # Try to find the correct bucket for news sentiment data
        news_bucket = None
        for bucket in buckets:
            if 'news' in bucket.lower() or 'sentiment' in bucket.lower():
                news_bucket = bucket
                break
        
        if not news_bucket:
            logger.warning("No news sentiment bucket found. Using first available bucket.")
            news_bucket = buckets[0]
        
        logger.info(f"Using bucket: {news_bucket}")
        
        # List all S3 objects
        all_objects = self.list_s3_objects(news_bucket)
        logger.info(f"Total S3 objects found: {len(all_objects)}")
        
        # Group objects by type
        raw_data_files = [obj for obj in all_objects if obj.startswith('raw_data/')]
        processed_data_files = [obj for obj in all_objects if obj.startswith('processed_data/')]
        sentiment_data_files = [obj for obj in all_objects if obj.startswith('sentiment_data/')]
        
        logger.info(f"Raw data files: {len(raw_data_files)}")
        logger.info(f"Processed data files: {len(processed_data_files)}")
        logger.info(f"Sentiment data files: {len(sentiment_data_files)}")
        
        # Preview latest raw data
        if raw_data_files:
            logger.info("\n📥 LATEST RAW DATA (sample):")
            latest_raw = sorted(raw_data_files)[-1]
            raw_data = self.read_json_from_s3(latest_raw, news_bucket)
            if raw_data:
                articles = raw_data.get('articles', [])
                logger.info(f"Found {len(articles)} articles in raw data")
                if articles:
                    # Show first 2-3 articles
                    for i, article in enumerate(articles[:3]):
                        logger.info(f"\nArticle {i+1}:")
                        logger.info(f"  Title: {article.get('title', 'N/A')[:100]}...")
                        logger.info(f"  Source: {article.get('source', 'N/A')}")
                        logger.info(f"  Keyword: {article.get('keyword', 'N/A')}")
                        logger.info(f"  URL: {article.get('url', 'N/A')[:80]}...")
        
        # Preview latest sentiment data
        if sentiment_data_files:
            logger.info("\n😊 LATEST SENTIMENT DATA (sample):")
            latest_sentiment = sorted(sentiment_data_files)[-1]
            sentiment_data = self.read_json_from_s3(latest_sentiment, news_bucket)
            if sentiment_data:
                individual_scores = sentiment_data.get('individual_scores', [])
                aggregate_scores = sentiment_data.get('aggregate_scores', {})
                
                logger.info(f"Found {len(individual_scores)} individual scores")
                logger.info(f"Found {len(aggregate_scores)} aggregate scores")
                
                if individual_scores:
                    logger.info("\nSample Individual Scores (2-3 records):")
                    for i, score in enumerate(individual_scores[:3]):
                        logger.info(f"\nScore {i+1}:")
                        logger.info(f"  Title: {score.get('title', 'N/A')[:80]}...")
                        logger.info(f"  Keyword: {score.get('keyword', 'N/A')}")
                        logger.info(f"  Mock Score: {score.get('mock_sentiment_score', 'N/A')}")
                        logger.info(f"  TextBlob Polarity: {score.get('textblob_sentiment', {}).get('polarity', 'N/A')}")
                
                if aggregate_scores:
                    logger.info("\nAggregate Scores:")
                    for keyword, scores in aggregate_scores.items():
                        logger.info(f"  {keyword}:")
                        logger.info(f"    Average: {scores.get('average_score', 'N/A')}")
                        logger.info(f"    Min: {scores.get('min_score', 'N/A')}")
                        logger.info(f"    Max: {scores.get('max_score', 'N/A')}")
                        logger.info(f"    Count: {scores.get('article_count', 'N/A')}")
        
        # Show all objects if no specific data found
        if not raw_data_files and not processed_data_files and not sentiment_data_files:
            logger.info("\n📁 ALL S3 OBJECTS (first 10):")
            for i, obj in enumerate(all_objects[:10]):
                logger.info(f"  {i+1}. {obj}")
            if len(all_objects) > 10:
                logger.info(f"  ... and {len(all_objects) - 10} more objects")
    
    def preview_summary_statistics(self):
        """Preview summary statistics"""
        logger.info("\n" + "="*60)
        logger.info("SUMMARY STATISTICS")
        logger.info("="*60)
        
        # Database statistics
        logger.info("\n📊 DATABASE STATISTICS:")
        
        # Total counts
        total_articles = self.query_database("SELECT COUNT(*) as count FROM articles")
        total_sentiment = self.query_database("SELECT COUNT(*) as count FROM sentiment_scores")
        total_aggregate = self.query_database("SELECT COUNT(*) as count FROM aggregate_scores")
        total_runs = self.query_database("SELECT COUNT(*) as count FROM pipeline_runs")
        
        logger.info(f"  Total Articles: {total_articles.iloc[0]['count'] if total_articles is not None and not total_articles.empty else 0}")
        logger.info(f"  Total Sentiment Scores: {total_sentiment.iloc[0]['count'] if total_sentiment is not None and not total_sentiment.empty else 0}")
        logger.info(f"  Total Aggregate Scores: {total_aggregate.iloc[0]['count'] if total_aggregate is not None and not total_aggregate.empty else 0}")
        logger.info(f"  Total Pipeline Runs: {total_runs.iloc[0]['count'] if total_runs is not None and not total_runs.empty else 0}")
        
        # Keyword distribution
        keyword_dist = self.query_database("""
            SELECT keyword, COUNT(*) as count 
            FROM articles 
            GROUP BY keyword 
            ORDER BY count DESC
        """)
        
        if keyword_dist is not None and not keyword_dist.empty:
            logger.info("\n📈 KEYWORD DISTRIBUTION:")
            logger.info(keyword_dist.to_string(index=False))
        else:
            logger.info("\n📈 KEYWORD DISTRIBUTION: No data available")
        
        # Source distribution
        source_dist = self.query_database("""
            SELECT source, COUNT(*) as count 
            FROM articles 
            GROUP BY source 
            ORDER BY count DESC
        """)
        
        if source_dist is not None and not source_dist.empty:
            logger.info("\n📰 SOURCE DISTRIBUTION:")
            logger.info(source_dist.to_string(index=False))
        else:
            logger.info("\n📰 SOURCE DISTRIBUTION: No data available")
        
        # Latest sentiment scores
        latest_scores = self.query_database("""
            SELECT keyword, average_score, article_count, date 
            FROM aggregate_scores 
            WHERE date = (SELECT MAX(date) FROM aggregate_scores)
            ORDER BY keyword
        """)
        
        if latest_scores is not None and not latest_scores.empty:
            logger.info("\n🎯 LATEST SENTIMENT SCORES:")
            logger.info(latest_scores.to_string(index=False))
        else:
            logger.info("\n🎯 LATEST SENTIMENT SCORES: No data available")
        
        # S3 statistics
        logger.info("\n☁️ S3 STORAGE STATISTICS:")
        buckets = self.list_s3_buckets()
        if buckets:
            news_bucket = None
            for bucket in buckets:
                if 'news' in bucket.lower() or 'sentiment' in bucket.lower():
                    news_bucket = bucket
                    break
            if not news_bucket:
                news_bucket = buckets[0]
            
            all_objects = self.list_s3_objects(news_bucket)
            raw_count = len([obj for obj in all_objects if obj.startswith('raw_data/')])
            processed_count = len([obj for obj in all_objects if obj.startswith('processed_data/')])
            sentiment_count = len([obj for obj in all_objects if obj.startswith('sentiment_data/')])
            
            logger.info(f"  Bucket: {news_bucket}")
            logger.info(f"  Raw Data Files: {raw_count}")
            logger.info(f"  Processed Data Files: {processed_count}")
            logger.info(f"  Sentiment Data Files: {sentiment_count}")
            logger.info(f"  Total S3 Objects: {len(all_objects)}")
        else:
            logger.info("  No S3 buckets available")
    
    def preview_all_outputs(self):
        """Preview all pipeline outputs"""
        logger.info("Starting News Sentiment Pipeline Output Preview")
        logger.info(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Check connections using the connection pool
        connection_status = self.check_connections()
        
        s3_connected = connection_status.get('s3', False)
        db_connected = connection_status.get('postgres', False)
        
        if not s3_connected and not db_connected:
            logger.error("Cannot proceed without S3/MinIO or database connection")
            return False
        
        # Preview database data if connected
        if db_connected:
            self.preview_database_data()
        else:
            logger.warning("Database not connected - skipping database preview")
        
        # Preview S3 data if connected
        if s3_connected:
            self.preview_s3_data()
        else:
            logger.warning("S3/MinIO not connected - skipping S3 preview")
        
        # Preview summary statistics
        self.preview_summary_statistics()
        
        # Summary
        logger.info("\n" + "="*60)
        logger.info("PREVIEW SUMMARY")
        logger.info("="*60)
        
        connections_status = {
            "Database Connection": db_connected,
            "S3/MinIO Connection": s3_connected
        }
        
        for connection_name, status in connections_status.items():
            status_str = "✓ CONNECTED" if status else "✗ DISCONNECTED"
            logger.info(f"  {connection_name}: {status_str}")
        
        if db_connected or s3_connected:
            logger.info("\n✅ Preview completed successfully!")
        else:
            logger.error("❌ No connections available for preview")
        
        return db_connected or s3_connected

def main():
    """Main function"""
    try:
        # Create preview instance using connection pool
        preview = NewsSentimentOutputPreview()
        
        # Run preview
        success = preview.preview_all_outputs()
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("Preview interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 