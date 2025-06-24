# Data Engineering Assignment Makefile
# Consolidates all common commands for development, testing, and deployment

.PHONY: help setup install test lint format clean docker-build docker-up docker-down deploy download-jars trigger-dag trigger-task list-dags preview-news-sentiment preview-movielens

# Default target
help:
	@echo "Data Engineering Assignment - Available Commands:"
	@echo ""
	@echo "Setup & Installation:"
	@echo "  setup          - Initial setup (download JARs, create venv)"
	@echo "  install        - Install Python dependencies"
	@echo "  download-jars  - Download Spark JARs"
	@echo ""
	@echo "Development:"
	@echo "  test           - Run all tests"
	@echo "  test-integration - Run integration tests only"
	@echo "  test-movielens - Test MovieLens pipeline components"
	@echo "  lint           - Run code linting"
	@echo "  format         - Format code with black"
	@echo "  format-check   - Check code formatting"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build   - Build Docker images"
	@echo "  docker-up      - Start Docker services"
	@echo "  docker-down    - Stop Docker services"
	@echo "  docker-logs    - Show Docker logs"
	@echo ""
	@echo "Airflow DAG Management:"
	@echo "  list-dags      - List all available DAGs and their tasks"
	@echo "  trigger-dag    - Trigger a DAG (usage: make trigger-dag DAG_ID=<dag_id>)"
	@echo "  trigger-task   - Trigger a specific task (usage: make trigger-task DAG_ID=<dag_id> TASK_ID=<task_id>)"
	@echo ""
	@echo "Deployment:"
	@echo "  deploy         - Deploy using deploy.sh script"
	@echo "  deploy-validate - Validate deployment"
	@echo "  deploy-test    - Run post-deployment tests"
	@echo ""
	@echo "Utilities:"
	@echo "  clean          - Clean up generated files"
	@echo "  preview-news-sentiment - Show preview of news sentiment pipeline output"
	@echo "  preview-movielens - Show preview of MovieLens analytics pipeline output"

# Setup & Installation
setup: download-jars
	@echo "Setting up development environment..."
	@if [ ! -d "venv" ]; then \
		python3 -m venv venv; \
		echo "Virtual environment created"; \
	else \
		echo "Virtual environment already exists"; \
	fi
	@echo "Virtual environment is ready"

install:
	@echo "Installing Python dependencies in virtual environment..."
	@if [ ! -d "venv" ]; then \
		echo "Error: Virtual environment not found. Run 'make setup' first."; \
		exit 1; \
	fi
	venv/bin/pip install --upgrade pip
	venv/bin/pip install -r requirements.txt
	@echo "Dependencies installed successfully in virtual environment!"

download-jars:
	@echo "Downloading Spark JARs..."
	@chmod +x scripts/download-spark-jars.sh
	./scripts/download-spark-jars.sh

# Development
test:
	@echo "Running all tests..."
	python -m pytest src/ -v --cov=src --cov-report=term-missing
	python src/test_alerts.py

test-unit:
	@echo "Running unit tests..."
	python -m pytest src/ -v -m "not integration" --cov=src --cov-report=term-missing

test-integration:
	@echo "Running integration tests..."
	python -m src.integration_test

lint:
	@echo "Running code linting (lenient mode)..."
	@if command -v flake8 >/dev/null 2>&1; then \
		flake8 src/ dags/ --count --select=E9,F63 --show-source --statistics --max-line-length=200 --max-complexity=20 --ignore=E203,E501,W503,F401,F403,F405; \
	else \
		echo "flake8 not found. Install with: pip install flake8"; \
	fi

format:
	@echo "Formatting code..."
	@if command -v black >/dev/null 2>&1; then \
		black src/ dags/; \
	else \
		echo "black not found. Install with: pip install black"; \
	fi

format-check:
	@echo "Checking code formatting..."
	@if command -v black >/dev/null 2>&1; then \
		black --check src/ dags/; \
	else \
		echo "black not found. Install with: pip install black"; \
	fi

# Docker
docker-build:
	@echo "Building Docker images..."
	docker-compose build --no-cache

docker-up:
	@echo "Starting Docker services..."
	docker-compose up -d

docker-down:
	@echo "Stopping Docker services..."
	docker-compose down

docker-logs:
	@echo "Showing Docker logs..."
	docker-compose logs -f

# Deployment
deploy:
	@echo "Deploying application..."
	@chmod +x scripts/deploy.sh
	./scripts/deploy.sh deploy

deploy-validate:
	@echo "Validating deployment..."
	@chmod +x scripts/deploy.sh
	./scripts/deploy.sh validate

deploy-test:
	@echo "Running post-deployment tests..."
	@chmod +x scripts/deploy.sh
	./scripts/deploy.sh test

clean:
	@echo "Cleaning up generated files..."
	rm -rf __pycache__/
	rm -rf src/__pycache__/
	rm -rf dags/__pycache__/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf coverage.xml
	rm -rf htmlcov/
	@echo "Cleanup completed!"

# CI/CD helpers
ci-test:
	@echo "Running CI tests..."
	python -m pytest src/ -v --cov=src --cov-report=xml
	python src/test_alerts.py

ci-lint:
	@echo "Running CI linting..."
	flake8 src/ dags/ --count --select=E9,F63 --show-source --statistics --max-line-length=200 --max-complexity=20 --ignore=E203,E501,W503,F401,F403,F405

ci-format-check:
	@echo "Running CI format check..."
	black --check src/ dags/

# Development workflow
dev-setup:
	@echo "Setting up development environment..."
	@if [ ! -d "venv" ]; then \
		python3 -m venv venv; \
		echo "Virtual environment created"; \
	else \
		echo "Virtual environment already exists"; \
	fi
	@echo "Installing dependencies in virtual environment..."
	@venv/bin/pip install --upgrade pip
	@venv/bin/pip install -r requirements.txt
	@echo "Development environment setup complete!"
	@echo "Next steps:"
	@echo "  1. Activate virtual environment: source venv/bin/activate"
	@echo "  2. Start Docker services: make docker-up"
	@echo "  3. Run tests: make test"

dev-start: docker-up
	@echo "Development environment started!"
	@echo "Airflow UI: http://localhost:8080"
	@echo "Spark Master: http://localhost:8081"

dev-stop: docker-down
	@echo "Development environment stopped!"

# Quick development cycle
dev-test: lint format-check test
	@echo "Development cycle completed!"

# Airflow DAG Management
list-dags:
	@echo "Available DAGs and Tasks:"
	@echo ""
	@echo "1. news_sentiment_pipeline"
	@echo "   Description: Pipeline to fetch news articles and generate sentiment scores for HDFC and Tata Motors"
	@echo "   Schedule: 7 PM every working day (Monday-Friday)"
	@echo "   Tasks:"
	@echo "     - extract_news_data"
	@echo "     - process_news_data"
	@echo "     - generate_sentiment_scores"
	@echo "     - persist_sentiment_data"
	@echo "     - test_failure (test task that can be made to fail)"
	@echo ""
	@echo "2. movielens_analytics_pipeline"
	@echo "   Description: Daily pipeline to process MovieLens ml-100k dataset using Spark for analytics"
	@echo "   Schedule: 8 PM Monday-Friday (waits for news sentiment pipeline to complete)"
	@echo "   Tasks:"
	@echo "     - create_spark_connection"
	@echo "     - check_spark_availability"
	@echo "     - wait_for_news_sentiment_pipeline (sensor waiting for news pipeline)"
	@echo "     - download_and_upload_movielens_data"
	@echo "     - verify_raw_data_s3"
	@echo "     - task1_mean_age_by_occupation (Spark task)"
	@echo "     - task2_top_rated_movies (Spark task)"
	@echo "     - task3_top_genres_by_age_occupation (Spark task)"
	@echo "     - task4_similar_movies (Spark task)"
	@echo ""
	@echo "Usage Examples:"
	@echo "  make trigger-dag DAG_ID=news_sentiment_pipeline"
	@echo "  make trigger-task DAG_ID=news_sentiment_pipeline TASK_ID=extract_news_data"
	@echo "  make trigger-task DAG_ID=movielens_analytics_pipeline TASK_ID=task1_mean_age_by_occupation"

trigger-dag:
	@if [ -z "$(DAG_ID)" ]; then \
		echo "Error: DAG_ID is required. Usage: make trigger-dag DAG_ID=<dag_id>"; \
		echo ""; \
		echo "Available DAGs:"; \
		echo "  - news_sentiment_pipeline"; \
		echo "  - movielens_analytics_pipeline"; \
		echo ""; \
		echo "Run 'make list-dags' for detailed information about DAGs and tasks."; \
		exit 1; \
	fi
	@echo "Triggering DAG: $(DAG_ID)"
	docker-compose exec -T airflow-scheduler airflow dags trigger $(DAG_ID)

trigger-task:
	@if [ -z "$(DAG_ID)" ] || [ -z "$(TASK_ID)" ]; then \
		echo "Error: Both DAG_ID and TASK_ID are required."; \
		echo "Usage: make trigger-task DAG_ID=<dag_id> TASK_ID=<task_id>"; \
		echo "Usage: make trigger-task DAG_ID=movielens_analytics_pipeline TASK_ID=check_news_sentiment_success"; \
		echo ""; \
		echo "Available DAGs:"; \
		echo "  - news_sentiment_pipeline"; \
		echo "  - movielens_analytics_pipeline"; \
		echo ""; \
		echo "Run 'make list-dags' for detailed information about DAGs and tasks."; \
		exit 1; \
	fi
	@echo "Triggering task: $(TASK_ID) in DAG: $(DAG_ID)"
	@echo "Note: This will test the task with current UTC time as execution date"
	docker-compose exec -T airflow-scheduler airflow tasks test $(DAG_ID) $(TASK_ID) $(shell date -u +%Y-%m-%dT%H:%M:%S+00:00)

# Utilities
preview-news-sentiment:
	@echo "Showing preview of news sentiment pipeline output..."
	python -m src.preview_news_sentiment_output

preview-movielens:
	@echo "Showing preview of MovieLens analytics pipeline output..."
	python -m src.preview_movielens_output 