import json
import os
import logging
from typing import List, Dict, Any
from datetime import datetime
from config import STORAGE_CONFIG, get_enabled_sources

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ScalableDataProcessor:
    """Scalable data processor that processes data for individual stocks"""
    
    def __init__(self):
        self.processed_data = []
    
    def process_stock_data(self, stock_name: str) -> Dict[str, Any]:
        """Process all extracted data for a specific stock"""
        logger.info(f"Processing data for stock: {stock_name}")
        
        # Load all extraction results for this stock
        all_articles = self._load_stock_extractions(stock_name)
        
        if not all_articles:
            logger.warning(f"No extraction data found for stock: {stock_name}")
            return {
                'stock_name': stock_name,
                'processed_articles': [],
                'processing_timestamp': datetime.now().isoformat(),
                'total_articles': 0,
                'processed_count': 0
            }
        
        logger.info(f"Found {len(all_articles)} total articles for {stock_name}")
        
        # Process each article
        processed_articles = []
        for article in all_articles:
            processed_article = self._process_article(article, stock_name)
            if processed_article:
                processed_articles.append(processed_article)
        
        # Remove duplicates based on URL
        processed_articles = self._remove_duplicates(processed_articles)
        
        result = {
            'stock_name': stock_name,
            'processed_articles': processed_articles,
            'processing_timestamp': datetime.now().isoformat(),
            'total_articles': len(all_articles),
            'processed_count': len(processed_articles)
        }
        
        # Save processed result
        self._save_processed_result(result, stock_name)
        
        logger.info(f"Processing completed for {stock_name}: {len(processed_articles)} processed from {len(all_articles)} total")
        return result
    
    def _load_stock_extractions(self, stock_name: str) -> List[Dict[str, Any]]:
        """Load all extraction results for a specific stock from all sources"""
        all_articles = []
        extraction_dir = os.path.join(STORAGE_CONFIG['local_temp_dir'], 'extractions')
        
        if not os.path.exists(extraction_dir):
            logger.warning(f"Extraction directory not found: {extraction_dir}")
            return all_articles
        
        # Get all enabled sources
        enabled_sources = get_enabled_sources()
        
        for source in enabled_sources:
            source_name = source['name']
            filename = f"{stock_name.lower().replace(' ', '_')}_{source_name.lower()}_extraction.json"
            filepath = os.path.join(extraction_dir, filename)
            
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        extraction_data = json.load(f)
                        articles = extraction_data.get('articles', [])
                        all_articles.extend(articles)
                        logger.info(f"Loaded {len(articles)} articles from {source_name} for {stock_name}")
                except Exception as e:
                    logger.error(f"Error loading extraction data from {filepath}: {e}")
            else:
                logger.debug(f"Extraction file not found: {filepath}")
        
        return all_articles
    
    def _process_article(self, article: Dict[str, Any], stock_name: str) -> Dict[str, Any]:
        """Process individual article"""
        try:
            processed = article.copy()
            
            # Add stock name if not present
            if 'stock_name' not in processed:
                processed['stock_name'] = stock_name
            
            # Clean title and content
            processed['title'] = self._clean_text(processed.get('title', ''))
            processed['content'] = self._clean_text(processed.get('content', ''))
            
            # Add processing metadata
            processed['processing_timestamp'] = datetime.now().isoformat()
            processed['content_length'] = len(processed['content'])
            processed['title_length'] = len(processed['title'])
            
            # Filter out articles with too little content
            if len(processed['content']) < 50:
                logger.debug(f"Article filtered out due to insufficient content: {processed['title'][:50]}...")
                return None
            
            return processed
            
        except Exception as e:
            logger.error(f"Error processing article: {e}")
            return None
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        
        # Convert to string if not already
        text = str(text)
        
        # Remove extra whitespace
        import re
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
        
        # Strip leading/trailing whitespace
        text = text.strip()
        
        return text
    
    def _remove_duplicates(self, articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate articles based on URL"""
        seen_urls = set()
        unique_articles = []
        
        for article in articles:
            url = article.get('url', '')
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_articles.append(article)
        
        logger.info(f"Removed {len(articles) - len(unique_articles)} duplicate articles")
        return unique_articles
    
    def _save_processed_result(self, result: Dict[str, Any], stock_name: str):
        """Save processed result to temporary file"""
        temp_dir = os.path.join(STORAGE_CONFIG['local_temp_dir'], 'processed')
        os.makedirs(temp_dir, exist_ok=True)
        
        filename = f"{stock_name.lower().replace(' ', '_')}_processed.json"
        filepath = os.path.join(temp_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved processed result to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save processed result: {e}")

# Global function for Airflow
def process_stock_data(stock_name: str) -> Dict[str, Any]:
    """
    Main function called by Airflow to process data for a specific stock
    """
    processor = ScalableDataProcessor()
    return processor.process_stock_data(stock_name)

if __name__ == "__main__":
    # Test the scalable processing
    processor = ScalableDataProcessor()
    
    # Test with HDFC
    result = processor.process_stock_data('HDFC')
    print(f"Processed {result['processed_count']} articles from {result['total_articles']} total for HDFC") 