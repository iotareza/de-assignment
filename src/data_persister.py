import json
import os
import logging
from typing import List, Dict, Any
from datetime import datetime
import pandas as pd
from .connection_pool import get_postgres_connection, get_s3_client
from .s3_storage import S3Storage

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataPersister:
    def __init__(self):
        self.s3_storage = S3Storage()
    
    def persist_sentiment_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main function to persist sentiment analysis results to PostgreSQL
        Returns: Dictionary containing persistence results
        """
        logger.info("Starting data persistence to PostgreSQL...")
        
        # Debug: Log the data structure received
        logger.info(f"Received data keys: {list(data.keys())}")
        logger.info(f"Data structure: {type(data)}")
        
        # Load sentiment data from S3
        sentiment_data = self._load_sentiment_data(data)
        if not sentiment_data:
            logger.error("No sentiment data found")
            return {'persisted_records': 0, 'persistence_timestamp': datetime.now().isoformat()}
        
        individual_scores = sentiment_data.get('individual_scores', [])
        aggregate_scores = sentiment_data.get('aggregate_scores', {})
        s3_sentiment_data_key = sentiment_data.get('s3_sentiment_data_key', '')
        
        # Extract S3 paths from individual scores
        s3_raw_data_key = None
        s3_processed_data_key = None
        if individual_scores:
            # Get the first score to extract S3 paths (they should be the same for all)
            first_score = individual_scores[0]
            s3_raw_data_key = first_score.get('s3_raw_data_path')
            s3_processed_data_key = first_score.get('s3_processed_data_path')
        
        logger.info(f"Loaded {len(individual_scores)} individual scores and {len(aggregate_scores)} aggregate scores")
        logger.info(f"S3 paths - Raw: {s3_raw_data_key}, Processed: {s3_processed_data_key}, Sentiment: {s3_sentiment_data_key}")
        
        # Create pipeline run record
        run_id = f"pipeline_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self._create_pipeline_run(run_id, s3_sentiment_data_key, s3_raw_data_key, s3_processed_data_key)
        
        # Persist individual sentiment scores
        individual_persisted = self._persist_individual_scores(individual_scores)
        
        # Persist aggregate scores
        aggregate_persisted = self._persist_aggregate_scores(aggregate_scores, s3_sentiment_data_key)
        
        # Update pipeline run record
        self._update_pipeline_run(run_id, individual_persisted, aggregate_persisted, s3_processed_data_key)
        
        # Generate summary report
        summary_report = self._generate_summary_report()
        
        result = {
            'individual_records_persisted': individual_persisted,
            'aggregate_records_persisted': aggregate_persisted,
            'total_records_persisted': individual_persisted + aggregate_persisted,
            'summary_report': summary_report,
            'pipeline_run_id': run_id,
            'persistence_timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Data persistence completed. Total records: {result['total_records_persisted']}")
        return result
    
    def _load_sentiment_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load and structure sentiment data for persistence
        """
        # The sentiment analyzer provides data in this structure:
        # {
        #   'sentiment_results': [...],  # individual scores
        #   'aggregate_scores': {...},   # aggregate scores
        #   'analysis_timestamp': '...',
        #   'articles_analyzed': 11,
        #   's3_sentiment_data_key': '...'
        # }
        
        # Extract individual scores (they're in 'sentiment_results' field)
        individual_scores = data.get('sentiment_results', [])
        aggregate_scores = data.get('aggregate_scores', {})
        s3_sentiment_data_key = data.get('s3_sentiment_data_key', '')
        
        return {
            'individual_scores': individual_scores,
            'aggregate_scores': aggregate_scores,
            's3_sentiment_data_key': s3_sentiment_data_key
        }
    
    def _create_pipeline_run(self, run_id: str, s3_sentiment_data_key: str, s3_raw_data_key: str = None, s3_processed_data_key: str = None):
        """
        Create a new pipeline run record
        """
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('''
                        INSERT INTO pipeline_runs 
                        (run_id, start_time, status, s3_raw_data_path, s3_processed_data_path, s3_sentiment_data_path, articles_extracted, articles_processed, articles_analyzed)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (run_id, datetime.now(), 'RUNNING', s3_raw_data_key, s3_processed_data_key, s3_sentiment_data_key, 0, 0, 0))
                    conn.commit()
                    logger.info(f"Created pipeline run record: {run_id}")
        except Exception as e:
            logger.error(f"Error creating pipeline run: {str(e)}")
    
    def _update_pipeline_run(self, run_id: str, individual_count: int, aggregate_count: int, s3_processed_data_key: str = None):
        """
        Update pipeline run record with completion status
        """
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('''
                        UPDATE pipeline_runs 
                        SET end_time = %s, status = %s, articles_analyzed = %s, s3_processed_data_path = COALESCE(%s, s3_processed_data_path)
                        WHERE run_id = %s
                    ''', (datetime.now(), 'COMPLETED', individual_count, s3_processed_data_key, run_id))
                    conn.commit()
                    logger.info(f"Updated pipeline run record: {run_id}")
        except Exception as e:
            logger.error(f"Error updating pipeline run: {str(e)}")
    
    def _persist_individual_scores(self, individual_scores: List[Dict[str, Any]]) -> int:
        """
        Persist individual sentiment scores to PostgreSQL
        """
        persisted_count = 0
        
        logger.info(f"Attempting to persist {len(individual_scores)} individual scores")
        
        if not individual_scores:
            logger.warning("No individual scores to persist")
            return 0
        
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    
                    for i, score in enumerate(individual_scores):
                        try:
                            logger.debug(f"Processing individual score {i+1}/{len(individual_scores)}: {score.get('title', 'No title')[:50]}...")
                            
                            # Insert article record (metadata + sample content, full content is in S3)
                            sample_content = score.get('title', '')[:150] if score.get('title') else ''
                            if len(sample_content) < 150 and score.get('content'):
                                sample_content += score.get('content', '')[:150-len(sample_content)]
                            
                            cursor.execute('''
                                INSERT INTO articles 
                                (article_hash, keyword, source, title, url, content_length, s3_raw_content_path, s3_processed_content_path, extraction_timestamp, processing_timestamp, sample_content)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (article_hash) DO NOTHING
                            ''', (
                                score.get('article_id'),
                                score.get('keyword'),
                                score.get('source'),
                                score.get('title'),
                                score.get('url'),
                                score.get('content_length'),
                                score.get('s3_raw_content_path'),  # Raw content path
                                score.get('s3_processed_content_path'),  # Processed content path
                                score.get('analysis_timestamp'),
                                score.get('analysis_timestamp'),
                                sample_content
                            ))
                            
                            # Insert sentiment score
                            cursor.execute('''
                                INSERT INTO sentiment_scores 
                                (article_hash, keyword, source, mock_sentiment_score, textblob_polarity, textblob_subjectivity, analysis_timestamp)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ''', (
                                score.get('article_id'),
                                score.get('keyword'),
                                score.get('source'),
                                score.get('mock_sentiment_score'),
                                score.get('textblob_sentiment', {}).get('polarity'),
                                score.get('textblob_sentiment', {}).get('subjectivity'),
                                score.get('analysis_timestamp')
                            ))
                            
                            persisted_count += 1
                            
                        except Exception as e:
                            logger.error(f"Error persisting individual score {i+1}: {str(e)}")
                            logger.error(f"Score data: {score}")
                            continue
                    
                    conn.commit()
                    logger.info(f"Persisted {persisted_count} individual sentiment scores")
                    
        except Exception as e:
            logger.error(f"Error in batch persistence: {str(e)}")
        
        return persisted_count
    
    def _persist_aggregate_scores(self, aggregate_scores: Dict[str, Any], s3_sentiment_data_key: str) -> int:
        """
        Persist aggregate sentiment scores to PostgreSQL
        """
        persisted_count = 0
        
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    current_date = datetime.now().date()
                    
                    for keyword, scores in aggregate_scores.items():
                        try:
                            cursor.execute('''
                                INSERT INTO aggregate_scores 
                                (keyword, date, average_score, min_score, max_score, article_count, s3_sentiment_data_path)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (keyword, date) DO UPDATE SET
                                average_score = EXCLUDED.average_score,
                                min_score = EXCLUDED.min_score,
                                max_score = EXCLUDED.max_score,
                                article_count = EXCLUDED.article_count,
                                s3_sentiment_data_path = EXCLUDED.s3_sentiment_data_path
                            ''', (
                                keyword,
                                current_date,
                                scores.get('average_score'),
                                scores.get('min_score'),
                                scores.get('max_score'),
                                scores.get('article_count'),
                                s3_sentiment_data_key
                            ))
                            
                            persisted_count += 1
                            
                        except Exception as e:
                            logger.error(f"Error persisting aggregate score for {keyword}: {str(e)}")
                            continue
                    
                    conn.commit()
                    logger.info(f"Persisted {persisted_count} aggregate sentiment scores")
                    
        except Exception as e:
            logger.error(f"Error in aggregate persistence: {str(e)}")
        
        return persisted_count
    
    def _generate_summary_report(self) -> Dict[str, Any]:
        """
        Generate a summary report of the current data
        """
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    
                    # Get total counts
                    cursor.execute('SELECT COUNT(*) FROM articles')
                    result = cursor.fetchone()
                    total_articles = result[0] if result else 0
                    
                    cursor.execute('SELECT COUNT(*) FROM sentiment_scores')
                    result = cursor.fetchone()
                    total_sentiment_scores = result[0] if result else 0
                    
                    cursor.execute('SELECT COUNT(*) FROM aggregate_scores')
                    result = cursor.fetchone()
                    total_aggregate_scores = result[0] if result else 0
                    
                    # Get latest aggregate scores
                    cursor.execute('''
                        SELECT keyword, average_score, article_count, date 
                        FROM aggregate_scores 
                        WHERE date = (SELECT MAX(date) FROM aggregate_scores)
                        ORDER BY keyword
                    ''')
                    latest_scores = cursor.fetchall()
                    
                    # Get keyword distribution
                    cursor.execute('''
                        SELECT keyword, COUNT(*) as count 
                        FROM articles 
                        GROUP BY keyword 
                        ORDER BY count DESC
                    ''')
                    keyword_distribution = cursor.fetchall()
                    
                    # Get source distribution
                    cursor.execute('''
                        SELECT source, COUNT(*) as count 
                        FROM articles 
                        GROUP BY source 
                        ORDER BY count DESC
                    ''')
                    source_distribution = cursor.fetchall()
                    
                    return {
                        'total_articles': total_articles,
                        'total_sentiment_scores': total_sentiment_scores,
                        'total_aggregate_scores': total_aggregate_scores,
                        'latest_scores': [{'keyword': row[0], 'average_score': float(row[1]), 'article_count': row[2], 'date': str(row[3])} for row in latest_scores],
                        'keyword_distribution': [{'keyword': row[0], 'count': row[1]} for row in keyword_distribution],
                        'source_distribution': [{'source': row[0], 'count': row[1]} for row in source_distribution],
                        'report_timestamp': datetime.now().isoformat()
                    }
                    
        except Exception as e:
            logger.error(f"Error generating summary report: {str(e)}")
            # Return a default report structure instead of empty dict
            return {
                'total_articles': 0,
                'total_sentiment_scores': 0,
                'total_aggregate_scores': 0,
                'latest_scores': [],
                'keyword_distribution': [],
                'source_distribution': [],
                'report_timestamp': datetime.now().isoformat(),
                'error': str(e)
            }

# Global function for Airflow
def persist_sentiment_data(**context):
    """
    Main function called by Airflow to persist sentiment data from S3 to database
    """
    from .s3_storage import S3Storage
    
    # Get S3 key from previous task
    if context:
        s3_key = context['task_instance'].xcom_pull(
            task_ids='generate_sentiment_scores', 
            key='sentiment_data_s3_key'
        )
    else:
        raise ValueError("S3 key is required. This function should be called through Airflow or with a valid S3 key.")
    
    # Load data from S3
    s3_storage = S3Storage()
    data = s3_storage.load_data(s3_key)
    
    if not data:
        raise ValueError(f"Could not load data from S3 key: {s3_key}")
    
    # Persist the data
    persister = DataPersister()
    result = persister.persist_sentiment_data(data)
    
    return result
