# Data Engineering Assignment - News Sentiment & MovieLens Analytics Pipeline

This project implements two comprehensive data pipelines using Apache Airflow, Apache Spark, and MinIO:

1. **News Sentiment Analysis Pipeline**: Fetches news articles about HDFC and Tata Motors, processes them, and generates sentiment scores
2. **MovieLens Analytics Pipeline**: Processes MovieLens ml-100k dataset to generate user demographics, movie ratings, and recommendation insights

## Table of Contents

- [Pipeline Overview](#pipeline-overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [News Sentiment Pipeline](#news-sentiment-pipeline)
- [MovieLens Analytics Pipeline](#movielens-analytics-pipeline)
- [Connection Pool Management](#connection-pool-management)
- [Pre-Extract Filter System](#pre-extract-filter-system)
- [Three-Step Architecture](#three-step-architecture)
- [Integration Testing](#integration-testing)
- [Alerting System](#alerting-system)
- [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)

## Pipeline Overview

### Pipeline 1: News Sentiment Analysis
- **Data Sources**: YourStory.com and Finshots.in
- **Keywords**: HDFC, Tata Motors
- **Schedule**: 7 PM every working day (Monday-Friday)
- **Storage**: Raw data in S3/MinIO, metadata and sentiment scores in PostgreSQL
- **Infrastructure**: Docker containers with Airflow, PostgreSQL, and MinIO

### Pipeline 2: MovieLens Analytics
- **Dataset**: MovieLens ml-100k dataset
- **Schedule**: 8 PM daily (waits for news pipeline completion)
- **Tasks**: 4 analytical tasks including user demographics, movie ratings, genre preferences, and collaborative filtering
- **Storage**: Results in MinIO S3 storage with PostgreSQL metadata

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Source   │    │   Data Source   │    │   Data Source   │    │   Data Source   │
│  (YourStory)    │    │   (Finshots)    │    │   (YourStory)   │    │   (Finshots)    │
│   HDFC Search   │    │   HDFC Search   │    │ Tata Motors     │    │ Tata Motors     │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │                       │
         └───────────────────────┼───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   Extract Data  │
                    │  (5 articles    │
                    │   per keyword)  │
                    └─────────────────┘
                                 │
                    ┌─────────────────┐
                    │  Process & Clean│
                    │  (Deduplication)│
                    └─────────────────┘
                                 │
                    ┌─────────────────┐
                    │ Sentiment       │
                    │ Analysis        │
                    │ (Mock API)      │
                    └─────────────────┘
                                 │
                    ┌─────────────────┐
                    │ Persist to DB   │
                    │ (PostgreSQL)    │
                    └─────────────────┘
```

## Quick Start with Docker

### 1. Clone and Setup
```bash
git clone <repository-url>
cd de-assignment
```

### 2. Environment Configuration
```bash
# Copy environment template
cp env.example .env

# Edit .env file with your configuration
# (Default values work for local development)
```

### 3. Start the Infrastructure
```bash
# Build and start all services
docker-compose up -d

# Check service status
docker-compose ps
```

### 4. Access Services
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **PostgreSQL**: localhost:5432 (postgres/postgres)
- **Spark Master**: http://localhost:8081

### 5. Enable the Pipelines
1. Open Airflow UI at http://localhost:8080
2. Login with admin/admin
3. Find the `news_sentiment_pipeline` and `movielens_analytics_pipeline` DAGs
4. Click "Unpause" to enable scheduling

## News Sentiment Pipeline

### Data Flow

1. **Extraction**: Fetches 5 latest articles per keyword from each source
2. **S3 Storage**: Raw data saved to S3/MinIO object storage
3. **Processing**: Cleans text, removes duplicates, filters quality
4. **Sentiment**: Generates sentiment scores using mock API
5. **Persistence**: Stores metadata and sentiment scores in PostgreSQL, content paths in S3

### Database Schema

#### Articles Table
```sql
CREATE TABLE articles (
    id SERIAL PRIMARY KEY,
    article_hash VARCHAR(255) UNIQUE NOT NULL,
    keyword VARCHAR(100) NOT NULL,
    source VARCHAR(100) NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    content_length INTEGER,
    s3_content_path VARCHAR(500),
    extraction_timestamp TIMESTAMP,
    processing_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### Sentiment Scores Table
```sql
CREATE TABLE sentiment_scores (
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
```

### Manual Execution

```bash
# Test data extraction
docker-compose exec airflow-webserver python -m src.data_extractor

# Test data processing
docker-compose exec airflow-webserver python -m src.data_processor

# Test sentiment analysis
docker-compose exec airflow-webserver python -m src.sentiment_analyzer

# Test data persistence
docker-compose exec airflow-webserver python -m src.data_persister
```

## MovieLens Analytics Pipeline

### Overview

The MovieLens Analytics Pipeline processes the MovieLens ml-100k dataset to generate insights about user demographics, movie ratings, genre preferences, and movie recommendations using collaborative filtering.

### Analytical Tasks

#### Task 1: Mean Age by Occupation
- **Objective**: Find the mean age of users in each occupation
- **Input**: User demographic data
- **Output**: Average age per occupation category
- **Technology**: Spark SQL aggregations

#### Task 2: Top Rated Movies
- **Objective**: Find the top 20 highest rated movies (rated at least 35 times)
- **Input**: Movie ratings and movie metadata
- **Output**: Top 20 movies with highest average ratings
- **Technology**: Spark SQL with filtering and aggregations

#### Task 3: Genre Preferences by Demographics
- **Objective**: Find top genres rated by users of each occupation in age groups
- **Input**: Ratings, user demographics, and movie genres
- **Age Groups**: 20-25, 25-35, 35-45, 45+
- **Output**: Genre preferences by occupation and age group
- **Technology**: Spark SQL with complex joins and aggregations

#### Task 4: Movie Similarity using SVD
- **Objective**: Find top 10 similar movies using collaborative filtering
- **Input**: User-movie ratings matrix
- **Algorithm**: Alternating Least Squares (ALS) with cosine similarity
- **Output**: Top 10 most similar movies for a given movie
- **Technology**: Spark ML ALS and custom similarity calculations

### Dataset Information

The MovieLens ml-100k dataset contains:
- **100,000 ratings** (1-5 scale) from 943 users on 1,682 movies
- **User demographics**: age, gender, occupation, zip code
- **Movie metadata**: title, release date, genres (19 categories)
- **Time period**: September 1997 - April 1998

### Output Files

All results are stored in MinIO S3 bucket `movielens-data`:
- `task1_mean_age_by_occupation.parquet`: Mean age by occupation
- `task2_top_rated_movies.parquet`: Top 20 highest rated movies
- `task3_top_genres_by_occupation_age.parquet`: Genre preferences by demographics
- `task4_similar_movies_svd.parquet`: Movie similarity recommendations
- `processing_summary_YYYYMMDD.csv`: Daily processing summary

## Connection Pool Management

### Overview

The Connection Pool Manager provides centralized connection management for PostgreSQL, Redis, and S3/MinIO services. It implements connection pooling to improve performance and resource utilization.

### Features

- **Singleton Pattern**: Ensures only one connection pool manager exists across the application
- **Connection Pooling**: PostgreSQL connection pool with configurable min/max connections
- **Redis Connection Pool**: Built-in connection pooling with configurable settings
- **S3/MinIO Support**: Unified interface for both AWS S3 and MinIO
- **Automatic Cleanup**: Context managers ensure proper connection cleanup
- **Health Monitoring**: Connection testing and statistics
- **Thread Safety**: Thread-safe singleton implementation

### Usage

```python
from src.connection_pool import (
    get_postgres_connection,
    get_redis_client,
    get_s3_client,
    test_connections,
    get_connection_stats
)

# PostgreSQL with context manager (automatic cleanup)
with get_postgres_connection() as conn:
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM articles")
        results = cursor.fetchall()

# Redis client
redis_client = get_redis_client()
if redis_client:
    redis_client.set("key", "value")
    value = redis_client.get("key")

# S3/MinIO client
s3_client = get_s3_client()
if s3_client:
    # Works with both AWS S3 and MinIO
    if hasattr(s3_client, 'list_buckets'):  # MinIO
        buckets = s3_client.list_buckets()
    else:  # AWS S3
        response = s3_client.list_buckets()
```

### Configuration

All configuration is loaded from environment variables:

```bash
# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=sentiment_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_MIN_CONNECTIONS=1
POSTGRES_MAX_CONNECTIONS=10

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=
REDIS_MAX_CONNECTIONS=10

# S3/MinIO Configuration
S3_ENDPOINT=localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_BUCKET_NAME=news-data
S3_USE_SSL=false
```

## Pre-Extract Filter System

### Overview

The pre-extract filter prevents duplicate URL processing by using a combination of database URL tracking and Redis bloom filter. It supports stock-specific URL tracking, allowing the same article URL to be processed for multiple relevant stocks.

### Key Features

- **Stock-Specific Tracking**: Same URL can be processed for different stocks
- **Bloom Filter**: Fast probabilistic URL checking (stored in Redis)
- **Database Tracking**: Definitive URL tracking in PostgreSQL
- **Two-tier Filtering**: Bloom filter first (fast), then database (definitive)

### Database Schema

```sql
CREATE TABLE url_tracking (
    id SERIAL PRIMARY KEY,
    stock_name VARCHAR(100) NOT NULL,
    url TEXT NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_name, url)
);
```

### Usage

```python
from src.pre_extract_filter import PreExtractFilter

# Initialize filter
filter_obj = PreExtractFilter()

# Filter URLs for HDFC
urls = ["https://example.com/article1", "https://example.com/article2"]
filtered_hdfc = filter_obj.filter_urls(urls, "HDFC")

# Filter URLs for Tata Motors (same URLs, different stock)
filtered_tata = filter_obj.filter_urls(urls, "Tata Motors")

# Mark URL as processed for specific stock
filter_obj.mark_url_as_processed("https://example.com/article1", "HDFC")
```

### Benefits

- **Performance**: Fast O(k) lookup time per stock
- **Accuracy**: No false negatives, low false positives
- **Scalability**: Persistent storage, configurable size
- **Multi-Stock Support**: Independent tracking per stock

## Three-Step Architecture

### Overview

The news sentiment pipeline has been restructured into a **3-step extraction process** with advanced filtering and caching mechanisms for better separation of concerns, improved performance, and enhanced scalability.

### Architecture Components

#### Step 1: URL Collection (`src/url_collector.py`)
- **Purpose**: Collect all relevant URLs for each stock from all sources
- **Features**: Gathers URLs from all enabled sources, adds metadata
- **Output**: JSON file with all collected URLs for each stock

#### Step 2: PreExtractor Filter (`src/pre_extractor_filter.py`)
- **Purpose**: Filter URLs using both database and Redis bloom filter
- **Features**: Two-tier filtering, caching, stock-specific tracking
- **Process**: Bloom filter check → Database check → Update filters

#### Step 3: Content Extraction (`src/content_extractor.py`)
- **Purpose**: Extract content from filtered URLs and mark as processed
- **Features**: Rate limiting, stock-specific processing, database updates

### Performance Benefits

| Aspect | Before | After |
|--------|--------|-------|
| **URL Discovery** | Per-task | Once per stock |
| **Filtering** | Per-URL | Batch processing |
| **Caching** | None | Redis + Database |
| **Parallelization** | Limited | Full parallel |
| **Error Recovery** | Restart entire task | Resume from any step |

### Airflow DAG Structure

```
URL Collection Tasks (Parallel)
    ↓
PreExtractor Filter Tasks (Parallel)
    ↓
Content Extraction Tasks (Parallel)
    ↓
Processing Tasks (Parallel)
    ↓
Sentiment Analysis Tasks (Parallel)
    ↓
Persistence Tasks (Parallel)
    ↓
Aggregation Task
    ↓
Notification Task
```

## Integration Testing

### Overview

The integration test simulates the complete Airflow DAG pipeline by mocking Airflow's XCom system for task communication, running all pipeline steps sequentially, and verifying data flow between tasks.

### Features

- **XCom Mock Implementation**: Simulates Airflow's XCom functionality
- **Complete Pipeline Testing**: Tests all pipeline steps in sequence
- **Data Flow Verification**: Validates data flow between tasks
- **Debug Logging**: Shows XCom push/pull operations for debugging

### How to Run

```bash
# Option 1: Direct Python Execution
cd src
python integration_test.py

# Option 2: Using the Test Script
python test_integration.py
```

### XCom Data Flow

The pipeline uses the following XCom keys for communication:

1. **extract_news_data** → **process_news_data**
   - Key: `extracted_data_s3_key`
   - Value: S3 key containing extracted articles

2. **process_news_data** → **generate_sentiment_scores**
   - Key: `processed_data_s3_key`
   - Value: S3 key containing processed articles

3. **generate_sentiment_scores** → **persist_sentiment_data**
   - Key: `sentiment_data_s3_key`
   - Value: S3 key containing sentiment analysis results

### Benefits

- **Fast Testing**: No need to start Airflow for testing
- **Debugging**: Easy to trace data flow and identify issues
- **Development**: Rapid iteration during development
- **CI/CD**: Can be integrated into automated testing pipelines

## Alerting System

### Overview

The alerting system provides comprehensive monitoring and notification capabilities for both pipelines. It automatically detects failures at both task and pipeline levels and logs detailed alerts with context information.

### Features

- **🔔 Automatic Failure Detection**: Task and pipeline failure detection
- **📊 Rich Context Information**: Error messages, stack traces, execution details
- **🚨 Severity Levels**: LOW, MEDIUM, HIGH, CRITICAL with visual indicators
- **📝 Local Logging**: Beautiful formatted console output

### Alert Examples

#### Task Failure Alert
```
================================================================================
🟠 PIPELINE ALERT: news_sentiment_pipeline - extract_news_data
================================================================================
🚨 Severity: HIGH
⏰ Timestamp: 2024-01-15T20:30:00Z
📋 Error: Failed to connect to news API: Connection timeout
📊 Context: {'execution_date': '2024-01-15T19:00:00Z', 'dag_run_id': 'manual__2024-01-15T19:00:00+00:00', 'retry_count': 2, 'alert_type': 'task_failure'}
================================================================================
```

### Testing

```bash
# Run the test script to simulate various failures
python src/test_alerts.py
```

### Production Deployment

For production deployment, you can easily extend the alerting service to integrate with real alerting services:

- **Slack Integration**: Webhook-based notifications
- **Email Integration**: SMTP-based email alerts
- **PagerDuty Integration**: API-based incident management
- **Custom Webhook**: Integration with any alerting service

## Monitoring and Troubleshooting

### Service URLs

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Spark UI**: http://localhost:8081
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **Airflow Flower**: http://localhost:5555

### Environment Variables

Key environment variables are configured in `docker-compose.yml`:
- `S3_ENDPOINT`: MinIO endpoint
- `S3_ACCESS_KEY`: MinIO access key
- `S3_SECRET_KEY`: MinIO secret key
- `SPARK_MASTER_URL`: Spark master URL

### Debug Commands

```bash
# Check service health
docker-compose ps

# View detailed logs
docker-compose logs -f [service-name]

# Access container shell
docker-compose exec [service-name] bash

# Check database connectivity
docker-compose exec postgres pg_isready -U postgres
```

### Common Issues

1. **Airflow not starting**: Check PostgreSQL connection and database initialization
2. **MinIO connection errors**: Verify MinIO service is running and accessible
3. **Permission errors**: Ensure proper file permissions in mounted volumes
4. **Memory issues**: Increase Docker memory limits if needed
5. **Port Conflicts**: Ensure ports 8080, 8081, 9000, 9001, 5432, 6379 are available

### Logs

```bash
# View Airflow logs
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler
docker-compose logs airflow-worker

# View Spark logs
docker-compose logs spark-master
docker-compose logs spark-worker

# View all logs
docker-compose logs -f
```

## Project Structure

```
de-assignment/
├── dags/
│   ├── news_sentiment_pipeline.py
│   └── movielens_analytics_pipeline.py
├── src/
│   ├── alerting_service.py
│   ├── connection_pool.py
│   ├── pre_extract_filter.py
│   ├── data_extractor.py
│   ├── data_processor.py
│   ├── sentiment_analyzer.py
│   ├── data_persister.py
│   └── spark_apps/
├── init-scripts/
│   ├── init-sentiment-db.sql
│   └── init-url-tracking.sql
├── Dockerfile
├── Dockerfile.spark
├── docker-compose.yml
├── scripts/
│   └── deploy.sh
├── requirements.txt
├── env.example
└── README.md
```

## Support

For issues and questions:
1. Check the logs for error messages
2. Verify all services are running
3. Ensure proper network connectivity between containers
4. Check environment variable configuration
5. Run integration tests to verify functionality 