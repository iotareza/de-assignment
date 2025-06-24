import pandas as pd
import numpy as np
from textblob import TextBlob
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import logging
from typing import Dict, List, Any
from .s3_storage import S3Storage
import random
import time
from datetime import datetime
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SentimentAnalyzer:
    def __init__(self):
        self.sentiment_scores = []
        self.s3_storage = S3Storage()
        
    def generate_sentiment_scores(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main function to generate sentiment scores for processed articles
        Returns: Dictionary containing sentiment analysis results and S3 key
        """
        logger.info("Starting sentiment analysis...")
        
        # Debug: Log the structure of received data
        logger.info(f"Received data keys: {list(data.keys())}")
        logger.info(f"Data structure: {type(data)}")
        
        # Get the list of individual article paths from the batch metadata
        individual_article_paths = data.get('individual_article_paths', [])
        logger.info(f"Found {len(individual_article_paths)} individual article paths to analyze")
        
        # Debug: Log the actual paths
        for i, path in enumerate(individual_article_paths):
            logger.debug(f"Article path {i+1}: {path}")
        
        # Load and analyze each individual article
        articles = []
        for i, s3_path in enumerate(individual_article_paths):
            try:
                logger.debug(f"Loading article {i+1}/{len(individual_article_paths)} from {s3_path}")
                article_data = self.s3_storage.load_data(s3_path)
                if article_data:
                    articles.append(article_data)
                    logger.debug(f"Successfully loaded article from {s3_path}")
                    # Debug: Check if s3_processed_content_path is present
                    processed_path = article_data.get('s3_processed_content_path')
                    if processed_path:
                        logger.debug(f"Article has s3_processed_content_path: {processed_path}")
                    else:
                        logger.warning(f"Article loaded from {s3_path} but has no s3_processed_content_path")
                else:
                    logger.warning(f"Could not load article from {s3_path} - returned None")
            except Exception as e:
                logger.error(f"Error loading article from {s3_path}: {str(e)}")
        
        logger.info(f"Successfully loaded {len(articles)} articles for sentiment analysis")
        
        # Analyze sentiment for each article
        for article in articles:
            sentiment_result = self._analyze_article_sentiment(article)
            if sentiment_result:
                self.sentiment_scores.append(sentiment_result)
        
        # Calculate aggregate scores by keyword
        aggregate_scores = self._calculate_aggregate_scores()
        
        # Save sentiment results to S3
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        detailed_results = {
            'individual_scores': self.sentiment_scores,
            'aggregate_scores': aggregate_scores,
            'analysis_timestamp': datetime.now().isoformat()
        }
        s3_key = self.s3_storage.save_sentiment_data(detailed_results, timestamp)
        
        result = {
            'sentiment_results': self.sentiment_scores,
            'aggregate_scores': aggregate_scores,
            'analysis_timestamp': datetime.now().isoformat(),
            'articles_analyzed': len(self.sentiment_scores),
            's3_sentiment_data_key': s3_key
        }
        
        logger.info(f"Sentiment analysis completed. Analyzed: {len(self.sentiment_scores)} articles")
        return result
    
    def _analyze_article_sentiment(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze sentiment for a single article using mock API
        """
        try:
            # Combine title and content for analysis
            text_for_analysis = f"{article.get('title', '')} {article.get('content', '')}"
            
            # Call mock sentiment API
            sentiment_score = self._call_mock_sentiment_api(text_for_analysis)
            
            # Also get TextBlob sentiment for comparison
            textblob_sentiment = self._get_textblob_sentiment(text_for_analysis)
            
            # Debug: Log the S3 paths from the article
            s3_processed_path = article.get('s3_processed_content_path')
            s3_raw_path = article.get('s3_raw_content_path')
            logger.debug(f"Article '{article.get('title', 'Unknown')[:30]}...' - Raw: {s3_raw_path}, Processed: {s3_processed_path}")
            
            result = {
                'article_id': article.get('content_hash', 'unknown'),
                'keyword': article.get('keyword', 'unknown'),
                'source': article.get('source', 'unknown'),
                'title': article.get('title', ''),
                'url': article.get('url', ''),
                's3_raw_content_path': article.get('s3_raw_content_path'),  # Raw content path
                's3_processed_content_path': article.get('s3_processed_content_path'),  # Processed content path
                's3_raw_data_path': article.get('s3_raw_data_path'),
                's3_processed_data_path': article.get('s3_processed_data_path'),
                'mock_sentiment_score': sentiment_score,
                'textblob_sentiment': textblob_sentiment,
                'analysis_timestamp': datetime.now().isoformat(),
                'content_length': article.get('content_length', 0)
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing sentiment for article: {str(e)}")
            return None
    
    def _call_mock_sentiment_api(self, text: str) -> float:
        """
        Mock sentiment API that returns a score between 0 and 1
        In a real implementation, this would call an external API
        """
        try:
            # Simulate API call delay
            time.sleep(random.uniform(0.1, 0.5))
            
            # Use TextBlob as a base for more realistic mock scores
            blob = TextBlob(text)
            
            # Convert TextBlob polarity (-1 to 1) to our scale (0 to 1)
            base_score = (blob.sentiment.polarity + 1) / 2
            
            # Add some randomness to simulate real API behavior
            noise = random.uniform(-0.1, 0.1)
            final_score = max(0.0, min(1.0, base_score + noise))
            
            # Log the API call (in real implementation, this would be the actual API call)
            logger.debug(f"Mock API called for text length {len(text)}, score: {final_score:.3f}")
            
            return round(final_score, 3)
            
        except Exception as e:
            logger.error(f"Error in mock sentiment API: {str(e)}")
            # Return neutral score on error
            return 0.5
    
    def _get_textblob_sentiment(self, text: str) -> Dict[str, float]:
        """
        Get TextBlob sentiment analysis for comparison
        """
        try:
            blob = TextBlob(text)
            return {
                'polarity': round(blob.sentiment.polarity, 3),  # -1 to 1
                'subjectivity': round(blob.sentiment.subjectivity, 3)  # 0 to 1
            }
        except Exception as e:
            logger.error(f"Error in TextBlob sentiment: {str(e)}")
            return {'polarity': 0.0, 'subjectivity': 0.0}
    
    def _calculate_aggregate_scores(self) -> Dict[str, Any]:
        """
        Calculate aggregate sentiment scores by keyword
        """
        aggregate_scores = {}
        
        # Group by keyword
        keyword_groups = {}
        for result in self.sentiment_scores:
            keyword = result.get('keyword', 'unknown')
            if keyword not in keyword_groups:
                keyword_groups[keyword] = []
            keyword_groups[keyword].append(result)
        
        # Calculate aggregates for each keyword
        for keyword, results in keyword_groups.items():
            if not results:
                continue
                
            scores = [r.get('mock_sentiment_score', 0.5) for r in results]
            
            aggregate_scores[keyword] = {
                'average_score': round(sum(scores) / len(scores), 3),
                'min_score': round(min(scores), 3),
                'max_score': round(max(scores), 3),
                'article_count': len(results),
                'scores': scores,
                'last_updated': datetime.now().isoformat()
            }
        
        return aggregate_scores

# Global function for Airflow
def generate_sentiment_scores(**context):
    """
    Main function called by Airflow to generate sentiment scores from S3 and save to S3
    """
    from datetime import datetime
    from .s3_storage import S3Storage
    
    # Get S3 key from previous task
    if context:
        s3_key = context['task_instance'].xcom_pull(
            task_ids='process_news_data', 
            key='processed_data_s3_key'
        )
    else:
        raise ValueError("S3 key is required. This function should be called through Airflow or with a valid S3 key.")
    
    # Load data from S3
    s3_storage = S3Storage()
    data = s3_storage.load_data(s3_key)
    
    if not data:
        raise ValueError(f"Could not load data from S3 key: {s3_key}")
    
    # Generate sentiment scores
    analyzer = SentimentAnalyzer()
    sentiment_data = analyzer.generate_sentiment_scores(data)
    
    # Save sentiment data to S3
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    sentiment_s3_key = f"sentiment_data/sentiment_data_{timestamp}.json"
    
    s3_storage = S3Storage()
    s3_storage.save_data(sentiment_data, sentiment_s3_key)
    
    # Pass S3 key to next task via XCom
    if context:
        context['task_instance'].xcom_push(key='sentiment_data_s3_key', value=sentiment_s3_key)
    
    return sentiment_s3_key

if __name__ == "__main__":
    # Test the sentiment analysis with a sample S3 key
    # Note: This requires a valid S3 key from a previous pipeline run
    print("This script requires an S3 key from a previous pipeline run.")
    print("To test, run the full pipeline through Airflow or provide a valid S3 key.")
    print("Example usage in Airflow: generate_sentiment_scores()") 