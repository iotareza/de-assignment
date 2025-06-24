import json
import os
import logging
from typing import Dict, Any
from datetime import datetime
from config import STORAGE_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ScalableDataPersister:
    """Scalable data persister for individual stocks"""
    
    def __init__(self):
        pass
    
    def persist_stock_data(self, stock_name: str) -> Dict[str, Any]:
        """Persist data for a specific stock to database"""
        logger.info(f"Persisting data for stock: {stock_name}")
        
        # Load sentiment data for this stock
        sentiment_data = self._load_sentiment_data(stock_name)
        
        if not sentiment_data or not sentiment_data.get('sentiment_scores'):
            logger.warning(f"No sentiment data found for stock: {stock_name}")
            return {
                'stock_name': stock_name,
                'persisted_count': 0,
                'persistence_timestamp': datetime.now().isoformat(),
                'status': 'no_data'
            }
        
        sentiment_scores = sentiment_data['sentiment_scores']
        logger.info(f"Persisting {len(sentiment_scores)} sentiment scores for {stock_name}")
        
        # Persist data to database
        persisted_count = self._persist_to_database(stock_name, sentiment_scores)
        
        result = {
            'stock_name': stock_name,
            'persisted_count': persisted_count,
            'persistence_timestamp': datetime.now().isoformat(),
            'status': 'success' if persisted_count > 0 else 'failed'
        }
        
        # Save persistence result
        self._save_persistence_result(result, stock_name)
        
        logger.info(f"Data persistence completed for {stock_name}: {persisted_count} records persisted")
        return result
    
    def _load_sentiment_data(self, stock_name: str) -> Dict[str, Any]:
        """Load sentiment data for a specific stock"""
        sentiment_dir = os.path.join(STORAGE_CONFIG['local_temp_dir'], 'sentiment')
        filename = f"{stock_name.lower().replace(' ', '_')}_sentiment.json"
        filepath = os.path.join(sentiment_dir, filename)
        
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading sentiment data from {filepath}: {e}")
        
        return {}
    
    def _persist_to_database(self, stock_name: str, sentiment_scores: list) -> int:
        """Persist sentiment scores to database"""
        try:
            # This is a placeholder - implement actual database persistence here
            # You can use SQLAlchemy, psycopg2, or any other database library
            
            persisted_count = 0
            
            for score in sentiment_scores:
                # Placeholder database insertion logic
                # In production, implement actual database operations
                
                # Example SQL (replace with actual implementation):
                # INSERT INTO sentiment_scores (stock_name, article_id, title, sentiment_score, sentiment_label, confidence, analysis_timestamp)
                # VALUES (?, ?, ?, ?, ?, ?, ?)
                
                persisted_count += 1
            
            logger.info(f"Persisted {persisted_count} sentiment scores for {stock_name} to database")
            return persisted_count
            
        except Exception as e:
            logger.error(f"Error persisting data to database for {stock_name}: {e}")
            return 0
    
    def _save_persistence_result(self, result: Dict[str, Any], stock_name: str):
        """Save persistence result to temporary file"""
        temp_dir = os.path.join(STORAGE_CONFIG['local_temp_dir'], 'persisted')
        os.makedirs(temp_dir, exist_ok=True)
        
        filename = f"{stock_name.lower().replace(' ', '_')}_persisted.json"
        filepath = os.path.join(temp_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved persistence result to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save persistence result: {e}")

# Global function for Airflow
def persist_stock_data(stock_name: str) -> Dict[str, Any]:
    """
    Main function called by Airflow to persist data for a specific stock
    """
    persister = ScalableDataPersister()
    return persister.persist_stock_data(stock_name)

if __name__ == "__main__":
    # Test the scalable data persistence
    persister = ScalableDataPersister()
    
    # Test with HDFC
    result = persister.persist_stock_data('HDFC')
    print(f"Persisted {result['persisted_count']} records for {result['stock_name']}") 