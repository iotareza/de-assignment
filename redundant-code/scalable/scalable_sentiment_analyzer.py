import json
import os
import logging
from typing import Dict, Any
from datetime import datetime
from config import STORAGE_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ScalableSentimentAnalyzer:
    """Scalable sentiment analyzer for individual stocks"""
    
    def __init__(self):
        pass
    
    def analyze_stock_sentiment(self, stock_name: str) -> Dict[str, Any]:
        """Analyze sentiment for a specific stock"""
        logger.info(f"Analyzing sentiment for stock: {stock_name}")
        
        # Load processed data for this stock
        processed_data = self._load_processed_data(stock_name)
        
        if not processed_data or not processed_data.get('processed_articles'):
            logger.warning(f"No processed data found for stock: {stock_name}")
            return {
                'stock_name': stock_name,
                'sentiment_scores': [],
                'analysis_timestamp': datetime.now().isoformat(),
                'total_articles': 0
            }
        
        articles = processed_data['processed_articles']
        logger.info(f"Analyzing sentiment for {len(articles)} articles of {stock_name}")
        
        # Analyze sentiment for each article
        sentiment_scores = []
        for article in articles:
            sentiment_score = self._analyze_article_sentiment(article)
            if sentiment_score:
                sentiment_scores.append(sentiment_score)
        
        result = {
            'stock_name': stock_name,
            'sentiment_scores': sentiment_scores,
            'analysis_timestamp': datetime.now().isoformat(),
            'total_articles': len(articles),
            'analyzed_count': len(sentiment_scores)
        }
        
        # Save sentiment result
        self._save_sentiment_result(result, stock_name)
        
        logger.info(f"Sentiment analysis completed for {stock_name}: {len(sentiment_scores)} scores from {len(articles)} articles")
        return result
    
    def _load_processed_data(self, stock_name: str) -> Dict[str, Any]:
        """Load processed data for a specific stock"""
        processed_dir = os.path.join(STORAGE_CONFIG['local_temp_dir'], 'processed')
        filename = f"{stock_name.lower().replace(' ', '_')}_processed.json"
        filepath = os.path.join(processed_dir, filename)
        
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading processed data from {filepath}: {e}")
        
        return {}
    
    def _analyze_article_sentiment(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze sentiment for a single article"""
        try:
            # This is a placeholder - implement actual sentiment analysis here
            # You can use TextBlob, VADER, or any other sentiment analysis library
            
            title = article.get('title', '')
            content = article.get('content', '')
            
            # Simple placeholder sentiment analysis
            # In production, use proper sentiment analysis libraries
            combined_text = f"{title} {content}"
            
            # Placeholder sentiment score (replace with actual analysis)
            sentiment_score = {
                'article_id': article.get('url', ''),
                'title': title,
                'sentiment_score': 0.0,  # Neutral
                'sentiment_label': 'neutral',
                'confidence': 0.5,
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            return sentiment_score
            
        except Exception as e:
            logger.error(f"Error analyzing sentiment for article: {e}")
            return None
    
    def _save_sentiment_result(self, result: Dict[str, Any], stock_name: str):
        """Save sentiment result to temporary file"""
        temp_dir = os.path.join(STORAGE_CONFIG['local_temp_dir'], 'sentiment')
        os.makedirs(temp_dir, exist_ok=True)
        
        filename = f"{stock_name.lower().replace(' ', '_')}_sentiment.json"
        filepath = os.path.join(temp_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved sentiment result to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save sentiment result: {e}")

# Global function for Airflow
def analyze_stock_sentiment(stock_name: str) -> Dict[str, Any]:
    """
    Main function called by Airflow to analyze sentiment for a specific stock
    """
    analyzer = ScalableSentimentAnalyzer()
    return analyzer.analyze_stock_sentiment(stock_name)

if __name__ == "__main__":
    # Test the scalable sentiment analysis
    analyzer = ScalableSentimentAnalyzer()
    
    # Test with HDFC
    result = analyzer.analyze_stock_sentiment('HDFC')
    print(f"Analyzed sentiment for {result['analyzed_count']} articles of {result['stock_name']}") 