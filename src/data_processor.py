import pandas as pd
import re
import logging
from typing import List, Dict, Any
from datetime import datetime
import hashlib
from .s3_storage import S3Storage
from .connection_pool import get_redis_client, get_postgres_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NewsDataProcessor:
    def __init__(self):
        self.processed_data = []
        self.s3_storage = S3Storage()
        
    def process_news_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main function to process and clean the extracted news data
        Returns: Dictionary containing processed data and S3 key
        """
        logger.info("Starting news data processing...")
        
        articles = data.get('articles', [])
        s3_content_paths = data.get('s3_content_paths', [])
        logger.info(f"Processing {len(articles)} articles with {len(s3_content_paths)} S3 content paths")
        
        # Fetch individual article content from S3
        articles_with_content = self._fetch_articles_from_s3(articles, s3_content_paths)
        
        # Process each article and save individually
        processed_count = 0
        for article in articles_with_content:
            processed_article = self._process_article(article)
            if processed_article:
                # Generate S3 key first
                individual_s3_key = self._generate_individual_s3_key(processed_article)
                
                # Set the processed content path BEFORE saving
                processed_article['s3_processed_content_path'] = individual_s3_key
                
                # Save individual article to S3
                success = self._save_individual_article(processed_article, individual_s3_key)
                if success:
                    self.processed_data.append(processed_article)
                    processed_count += 1
                    logger.info(f"Processed and saved article {processed_count}: {processed_article.get('title', 'Unknown')[:50]}... -> {individual_s3_key}")
                else:
                    logger.error(f"Failed to save article: {processed_article.get('title', 'Unknown')[:50]}...")
            else:
                logger.warning(f"Article filtered out during processing: {article.get('title', 'Unknown')[:50]}...")
        
        logger.info(f"Processing step completed: {processed_count} articles processed and saved individually")
        
        # Remove duplicates
        self.processed_data = self._remove_duplicates(self.processed_data)
        
        # Save batch metadata to S3 (for tracking purposes)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        batch_s3_key = f"processed_data/batch_metadata_{timestamp}.json"
        
        individual_paths = [article.get('s3_processed_content_path') for article in self.processed_data]
        logger.info(f"Saving batch metadata with {len(individual_paths)} individual article paths")
        logger.debug(f"Individual article paths: {individual_paths}")
        
        # Debug: Check if any paths are None or empty
        none_paths = [i for i, path in enumerate(individual_paths) if not path]
        if none_paths:
            logger.warning(f"Found {len(none_paths)} articles with None/empty s3_processed_content_path at indices: {none_paths}")
        
        batch_metadata = {
            'processed_articles_count': len(self.processed_data),
            'processing_timestamp': datetime.now().isoformat(),
            'original_count': len(articles),
            'processed_count': len(self.processed_data),
            'individual_article_paths': individual_paths
        }
        
        logger.debug(f"Batch metadata content: {batch_metadata}")
        self.s3_storage.save_data(batch_metadata, batch_s3_key)
        
        # Add batch metadata S3 path to each article
        for article in self.processed_data:
            article['s3_processed_data_path'] = batch_s3_key  # Track batch metadata path
        
        result = {
            'processed_articles': self.processed_data,
            'processing_timestamp': datetime.now().isoformat(),
            'original_count': len(articles),
            'processed_count': len(self.processed_data),
            's3_processed_data_key': batch_s3_key
        }
        
        logger.info(f"Processing completed. Original: {len(articles)}, Processed: {len(self.processed_data)}")
        return result
    
    def _fetch_articles_from_s3(self, articles: List[Dict[str, Any]], s3_paths: List[str]) -> List[Dict[str, Any]]:
        """
        Fetch individual article content from S3 using the provided paths
        """
        articles_with_content = []
        
        for i, article in enumerate(articles):
            try:
                # Get the corresponding S3 path
                s3_path = article.get('s3_raw_content_path')
                if not s3_path:
                    logger.warning(f"No S3 path found for article {i}: {article.get('title', 'Unknown')}")
                    continue
                
                # Fetch content from S3
                article_content = self.s3_storage.load_data(s3_path)
                if article_content:
                    # Merge the content with the article metadata
                    article_with_content = article.copy()
                    article_with_content['full_content'] = article_content.get('content', '')
                    article_with_content['full_title'] = article_content.get('title', '')
                    articles_with_content.append(article_with_content)
                    logger.debug(f"Fetched content for article {i}: {article.get('title', 'Unknown')[:50]}...")
                else:
                    logger.warning(f"Could not fetch content from S3 path: {s3_path}")
                    # Keep the article with truncated content
                    articles_with_content.append(article)
                    
            except Exception as e:
                logger.error(f"Error fetching article content from S3: {str(e)}")
                # Keep the article with truncated content
                articles_with_content.append(article)
        
        logger.info(f"Fetched content for {len(articles_with_content)} articles from S3")
        return articles_with_content
    
    def _process_article(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process individual article: clean text, remove noise, etc.
        """
        try:
            processed = article.copy()
            
            # Use full content if available, otherwise use truncated content
            full_content = article.get('full_content', '')
            full_title = article.get('full_title', '')
            
            # Clean title (use full title if available)
            if full_title:
                processed['title'] = self._clean_text(full_title)
            else:
                processed['title'] = self._clean_text(article.get('title', ''))
            
            # Clean content (use full content if available)
            if full_content:
                processed['content'] = self._clean_text(full_content)
            else:
                processed['content'] = self._clean_text(article.get('content', ''))
            
            # Use content_hash from extractor if available, otherwise generate new one
            if article.get('content_hash'):
                processed['content_hash'] = article.get('content_hash')
            else:
                processed['content_hash'] = self._generate_content_hash(processed['title'], processed['content'])
            
            # Preserve raw content path
            if article.get('s3_raw_content_path'):
                processed['s3_raw_content_path'] = article.get('s3_raw_content_path')
            
            # Preserve other S3 paths
            if article.get('s3_raw_data_path'):
                processed['s3_raw_data_path'] = article.get('s3_raw_data_path')
            
            # Add processing metadata
            processed['processing_timestamp'] = datetime.now().isoformat()
            processed['content_length'] = len(processed['content'])
            processed['title_length'] = len(processed['title'])
            
            # Filter out articles with too little content
            if len(processed['content']) < 50:
                logger.warning(f"Article filtered out due to insufficient content: {processed['title'][:50]}...")
                return None
            
            return processed
            
        except Exception as e:
            logger.error(f"Error processing article: {str(e)}")
            return None
    
    def _clean_text(self, text: str) -> str:
        """
        Clean and normalize text content
        """
        if not text:
            return ""
        
        # Convert to string if not already
        text = str(text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove special characters but keep basic punctuation
        text = re.sub(r'[^\w\s\.\,\!\?\;\:\-\(\)]', '', text)
        
        # Remove multiple periods, commas, etc.
        text = re.sub(r'\.{2,}', '.', text)
        text = re.sub(r'\,{2,}', ',', text)
        
        # Remove common HTML artifacts
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        
        # Remove common website artifacts
        text = re.sub(r'Share this article', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Follow us on', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Subscribe to', '', text, flags=re.IGNORECASE)
        
        # Strip leading/trailing whitespace
        text = text.strip()
        
        return text
    
    def _generate_content_hash(self, title: str, content: str) -> str:
        """
        Generate a hash of title and content for deduplication
        """
        combined_text = f"{title.lower()}{content.lower()}"
        return hashlib.md5(combined_text.encode()).hexdigest()
    
    def _remove_duplicates(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicate articles based on content hash, checking both current batch and database
        Uses Redis as cache to avoid slow database queries
        """
        if not articles:
            return []
        
        # Get content hashes from current batch
        content_hashes = [article.get('content_hash') for article in articles if article.get('content_hash')]
        logger.info(f"Checking {len(content_hashes)} content hashes for duplicates")
        
        # Remove duplicates within current batch first
        seen_hashes = set()
        unique_articles = []
        batch_duplicates = 0
        
        for article in articles:
            content_hash = article.get('content_hash')
            if content_hash and content_hash not in seen_hashes:
                seen_hashes.add(content_hash)
                unique_articles.append(article)
            elif not content_hash:
                logger.warning(f"Article without content hash: {article.get('title', 'Unknown')}")
                unique_articles.append(article)
            else:
                batch_duplicates += 1
        
        logger.info(f"Removed {batch_duplicates} duplicates within current batch")
        
        # Now check against database using Redis cache
        unique_articles = self._check_database_duplicates(unique_articles)
        
        return unique_articles
    
    def _check_database_duplicates(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Check for duplicates in database using Redis cache
        """
        if not articles:
            return []
        
        try:
            redis_client = get_redis_client()
            if not redis_client:
                logger.warning("Redis not available, skipping database duplicate check")
                return articles
            
            # Get content hashes to check
            content_hashes = [article.get('content_hash') for article in articles if article.get('content_hash')]
            if not content_hashes:
                return articles
            
            logger.info(f"Checking {len(content_hashes)} content hashes against database")
            
            # Check Redis cache first
            existing_hashes = set()
            hashes_to_check = []
            
            for content_hash in content_hashes:
                cache_key = f"article_hash:{content_hash}"
                if redis_client.exists(cache_key):
                    existing_hashes.add(content_hash)
                    logger.debug(f"Found content hash {content_hash} in Redis cache")
                else:
                    hashes_to_check.append(content_hash)
            
            logger.info(f"Found {len(existing_hashes)} hashes in Redis cache, checking {len(hashes_to_check)} in database")
            
            # Bulk query database for hashes not in Redis
            if hashes_to_check:
                db_existing_hashes = self._bulk_check_database_hashes(hashes_to_check)
                existing_hashes.update(db_existing_hashes)
                
                # Update Redis cache with database results
                self._update_redis_cache(hashes_to_check, db_existing_hashes)
            
            # Remove articles that already exist in database
            final_articles = []
            db_duplicates = 0
            
            for article in articles:
                content_hash = article.get('content_hash')
                if content_hash and content_hash in existing_hashes:
                    db_duplicates += 1
                    logger.debug(f"Removing duplicate article: {article.get('title', 'Unknown')[:50]}... (hash: {content_hash})")
                else:
                    final_articles.append(article)
            
            logger.info(f"Removed {db_duplicates} duplicates found in database")
            logger.info(f"Final unique articles: {len(final_articles)}")
            
            return final_articles
            
        except Exception as e:
            logger.error(f"Error checking database duplicates: {str(e)}")
            logger.warning("Returning articles without database duplicate check")
            return articles
    
    def _bulk_check_database_hashes(self, content_hashes: List[str]) -> set:
        """
        Bulk query database to check which content hashes already exist
        """
        try:
            with get_postgres_connection() as conn:
                with conn.cursor() as cursor:
                    # Create placeholders for the IN clause
                    placeholders = ','.join(['%s'] * len(content_hashes))
                    query = f"SELECT article_hash FROM articles WHERE article_hash IN ({placeholders})"
                    
                    cursor.execute(query, content_hashes)
                    results = cursor.fetchall()
                    
                    existing_hashes = {row[0] for row in results}
                    logger.info(f"Found {len(existing_hashes)} existing hashes in database")
                    
                    return existing_hashes
                    
        except Exception as e:
            logger.error(f"Error in bulk database check: {str(e)}")
            return set()
    
    def _update_redis_cache(self, checked_hashes: List[str], existing_hashes: set):
        """
        Update Redis cache with database query results
        """
        try:
            redis_client = get_redis_client()
            if not redis_client:
                return
            
            # Set cache entries for all checked hashes
            # Existing hashes get a longer TTL, non-existing get a shorter TTL
            for content_hash in checked_hashes:
                cache_key = f"article_hash:{content_hash}"
                if content_hash in existing_hashes:
                    # Article exists in database - cache for longer (1 hour)
                    redis_client.setex(cache_key, 3600, "1")
                else:
                    # Article doesn't exist - cache for shorter time (10 minutes)
                    redis_client.setex(cache_key, 600, "0")
            
            logger.debug(f"Updated Redis cache for {len(checked_hashes)} content hashes")
            
        except Exception as e:
            logger.error(f"Error updating Redis cache: {str(e)}")
    
    def _generate_individual_s3_key(self, article: Dict[str, Any]) -> str:
        """
        Generate unique S3 key for individual article
        Returns: S3 key string
        """
        # Generate unique S3 key using timestamp and content hash
        # Use microsecond precision to ensure uniqueness
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]  # Include milliseconds
        content_hash = article.get('content_hash', 'unknown')
        s3_key = f"processed_articles/{timestamp}_{content_hash}.json"
        return s3_key
    
    def _save_individual_article(self, article: Dict[str, Any], s3_key: str) -> bool:
        """
        Save individual processed article to S3
        Returns: True if successful, False otherwise
        """
        try:
            logger.debug(f"Saving individual article '{article.get('title', 'Unknown')[:30]}...' to {s3_key}")
            
            # Save the individual article
            success = self.s3_storage.save_data(article, s3_key)
            if success:
                logger.debug(f"Successfully saved individual article to S3: {s3_key}")
                return True
            else:
                logger.error(f"Failed to save individual article to S3: {s3_key}")
                return False
            
        except Exception as e:
            logger.error(f"Error saving individual article to S3: {str(e)}")
            return False

# Global function for Airflow
def process_news_data(**context):
    """
    Main function called by Airflow to process news data from S3 and save processed data to S3
    """
    from datetime import datetime
    from .s3_storage import S3Storage
    
    # Get S3 key from previous task
    if context:
        s3_key = context['task_instance'].xcom_pull(
            task_ids='extract_news_data', 
            key='extracted_data_s3_key'
        )
    else:
        raise ValueError("S3 key is required. This function should be called through Airflow or with a valid S3 key.")
    
    # Load data from S3
    s3_storage = S3Storage()
    data = s3_storage.load_data(s3_key)
    
    if not data:
        raise ValueError(f"Could not load data from S3 key: {s3_key}")
    
    # Process the data (individual articles are already saved to S3)
    processor = NewsDataProcessor()
    processed_data = processor.process_news_data(data)
    
    # The processed_data already contains the batch metadata S3 key
    batch_s3_key = processed_data.get('s3_processed_data_key')
    
    # Pass S3 key to next task via XCom
    if context:
        context['task_instance'].xcom_push(key='processed_data_s3_key', value=batch_s3_key)
    
    return batch_s3_key