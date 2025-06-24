import json
import os
import logging
from typing import Dict, Any, List
from datetime import datetime
from src.config import STORAGE_CONFIG, STOCKS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ScalableAggregator:
    """Scalable aggregator that combines results from all stocks"""
    
    def __init__(self):
        pass
    
    def aggregate_all_results(self) -> Dict[str, Any]:
        """Aggregate results from all stocks"""
        logger.info("Aggregating results from all stocks")
        
        all_results = {}
        total_articles = 0
        total_sentiment_scores = 0
        total_persisted = 0
        
        for stock in STOCKS:
            stock_name = stock['name']
            
            # Load persistence result for this stock
            persistence_result = self._load_persistence_result(stock_name)
            
            if persistence_result:
                all_results[stock_name] = persistence_result
                total_articles += persistence_result.get('total_articles', 0)
                total_sentiment_scores += persistence_result.get('total_sentiment_scores', 0)
                total_persisted += persistence_result.get('persisted_count', 0)
            else:
                logger.warning(f"No persistence result found for {stock_name}")
        
        aggregated_result = {
            'aggregation_timestamp': datetime.now().isoformat(),
            'total_stocks': len(STOCKS),
            'processed_stocks': len(all_results),
            'total_articles': total_articles,
            'total_sentiment_scores': total_sentiment_scores,
            'total_persisted': total_persisted,
            'stock_results': all_results
        }
        
        # Save aggregated result
        self._save_aggregated_result(aggregated_result)
        
        logger.info(f"Aggregation completed: {len(all_results)} stocks processed, {total_persisted} total records persisted")
        return aggregated_result
    
    def _load_persistence_result(self, stock_name: str) -> Dict[str, Any]:
        """Load persistence result for a specific stock"""
        persisted_dir = os.path.join(STORAGE_CONFIG['local_temp_dir'], 'persisted')
        filename = f"{stock_name.lower().replace(' ', '_')}_persisted.json"
        filepath = os.path.join(persisted_dir, filename)
        
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading persistence result from {filepath}: {e}")
        
        return {}
    
    def _save_aggregated_result(self, result: Dict[str, Any]):
        """Save aggregated result to temporary file"""
        temp_dir = STORAGE_CONFIG['local_temp_dir']
        filename = f"aggregated_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = os.path.join(temp_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved aggregated result to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save aggregated result: {e}")

# Global function for Airflow
def aggregate_all_results() -> Dict[str, Any]:
    """
    Main function called by Airflow to aggregate all results
    """
    aggregator = ScalableAggregator()
    return aggregator.aggregate_all_results()

if __name__ == "__main__":
    # Test the scalable aggregation
    aggregator = ScalableAggregator()
    
    # Test aggregation
    result = aggregator.aggregate_all_results()
    print(f"Aggregated results for {result['processed_stocks']} stocks")
    print(f"Total persisted records: {result['total_persisted']}") 