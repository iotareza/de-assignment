-- Create sentiment database
CREATE DATABASE sentiment_db;

-- Create user for sentiment database (optional, using postgres user for simplicity)
-- CREATE USER sentiment_user WITH PASSWORD 'sentiment_password';
-- GRANT ALL PRIVILEGES ON DATABASE sentiment_db TO sentiment_user;

-- Connect to sentiment database
\c sentiment_db;

-- Create articles table
CREATE TABLE IF NOT EXISTS articles (
    id SERIAL PRIMARY KEY,
    article_hash VARCHAR(255) UNIQUE NOT NULL,
    keyword VARCHAR(100) NOT NULL,
    source VARCHAR(100) NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    content_length INTEGER,
    s3_raw_content_path VARCHAR(500),
    s3_processed_content_path VARCHAR(500),
    sample_content TEXT,
    extraction_timestamp TIMESTAMP,
    processing_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create sentiment_scores table
CREATE TABLE IF NOT EXISTS sentiment_scores (
    id SERIAL PRIMARY KEY,
    article_hash VARCHAR(255) NOT NULL,
    keyword VARCHAR(100) NOT NULL,
    source VARCHAR(100) NOT NULL,
    mock_sentiment_score DECIMAL(5,3) NOT NULL,
    textblob_polarity DECIMAL(5,3),
    textblob_subjectivity DECIMAL(5,3),
    analysis_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (article_hash) REFERENCES articles (article_hash)
);

-- Create aggregate_scores table
CREATE TABLE IF NOT EXISTS aggregate_scores (
    id SERIAL PRIMARY KEY,
    keyword VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    average_score DECIMAL(5,3) NOT NULL,
    min_score DECIMAL(5,3) NOT NULL,
    max_score DECIMAL(5,3) NOT NULL,
    article_count INTEGER NOT NULL,
    s3_sentiment_data_path VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(keyword, date)
);

-- Create pipeline_runs table
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(255) UNIQUE NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    s3_raw_data_path VARCHAR(500),
    s3_processed_data_path VARCHAR(500),
    s3_sentiment_data_path VARCHAR(500),
    articles_extracted INTEGER DEFAULT 0,
    articles_processed INTEGER DEFAULT 0,
    articles_analyzed INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_articles_keyword ON articles (keyword);
CREATE INDEX IF NOT EXISTS idx_articles_source ON articles (source);
CREATE INDEX IF NOT EXISTS idx_sentiment_keyword ON sentiment_scores (keyword);
CREATE INDEX IF NOT EXISTS idx_aggregate_keyword_date ON aggregate_scores (keyword, date);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs (status);

-- Grant permissions (if using separate user)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO sentiment_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO sentiment_user; 