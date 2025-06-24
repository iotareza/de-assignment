#!/bin/bash

# Deployment script for Data Engineering Pipeline
# This script deploys the latest code to the target environment

set -e  # Exit on any error

# Configuration
DEPLOYMENT_DIR="/opt/de-assignment"
BACKUP_DIR="/opt/de-assignment-backup"
LOG_FILE="/var/log/deployment.log"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"
    exit 1
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"
}

# Function to check if running as root
check_permissions() {
    if [[ $EUID -eq 0 ]]; then
        error "This script should not be run as root"
    fi
}

# Function to create backup
create_backup() {
    log "Creating backup of current deployment..."
    if [ -d "$DEPLOYMENT_DIR" ]; then
        sudo cp -r "$DEPLOYMENT_DIR" "$BACKUP_DIR-$TIMESTAMP"
        success "Backup created at $BACKUP_DIR-$TIMESTAMP"
    else
        warning "No existing deployment found, skipping backup"
    fi
}

# Function to stop services
stop_services() {
    log "Stopping Airflow services..."
    cd "$DEPLOYMENT_DIR" 2>/dev/null || true
    if [ -f "docker-compose.yml" ]; then
        docker-compose down || warning "Failed to stop services"
    fi
}

# Function to deploy new code
deploy_code() {
    log "Deploying new code..."
    
    # Create deployment directory if it doesn't exist
    sudo mkdir -p "$DEPLOYMENT_DIR"
    
    # Copy new code
    sudo cp -r dags/ "$DEPLOYMENT_DIR/"
    sudo cp -r src/ "$DEPLOYMENT_DIR/"
    sudo cp -r init-scripts/ "$DEPLOYMENT_DIR/"
    sudo cp -r spark-jars/ "$DEPLOYMENT_DIR/"
    sudo cp docker-compose.yml "$DEPLOYMENT_DIR/"
    sudo cp Dockerfile "$DEPLOYMENT_DIR/"
    sudo cp Dockerfile.spark "$DEPLOYMENT_DIR/"
    sudo cp requirements.txt "$DEPLOYMENT_DIR/"
    
    # Set proper permissions
    sudo chown -R $USER:$USER "$DEPLOYMENT_DIR"
    sudo chmod -R 755 "$DEPLOYMENT_DIR"
    
    success "Code deployed successfully"
}

# Function to start services
start_services() {
    log "Starting Airflow services..."
    cd "$DEPLOYMENT_DIR"
    
    # Build and start services
    docker-compose build --no-cache
    docker-compose up -d
    
    # Wait for services to be ready
    log "Waiting for services to be ready..."
    sleep 30
    
    # Check if services are running
    if docker-compose ps | grep -q "Up"; then
        success "Services started successfully"
    else
        error "Failed to start services"
    fi
}

# Function to validate deployment
validate_deployment() {
    log "Validating deployment..."
    
    # Check if required files exist
    local required_files=(
        "dags/news_sentiment_pipeline.py"
        "dags/movielens_analytics_pipeline.py"
        "src/alerting_service.py"
        "src/data_utils.py"
        "docker-compose.yml"
    )
    
    for file in "${required_files[@]}"; do
        if [ -f "$DEPLOYMENT_DIR/$file" ]; then
            log "✅ $file found"
        else
            error "❌ $file missing"
        fi
    done
    
    # Check if Airflow is accessible
    if curl -f http://localhost:8080 >/dev/null 2>&1; then
        success "Airflow web UI is accessible"
    else
        warning "Airflow web UI is not accessible yet"
    fi
    
    # Check if Spark is accessible
    if curl -f http://localhost:8081 >/dev/null 2>&1; then
        success "Spark master is accessible"
    else
        warning "Spark master is not accessible yet"
    fi
}

# Function to run post-deployment tests
run_tests() {
    log "Running post-deployment tests..."
    
    cd "$DEPLOYMENT_DIR"
    
    # Test alerting system
    if python3 src/test_alerts.py; then
        success "Alerting system test passed"
    else
        warning "Alerting system test failed"
    fi
    
    # Test DAG syntax
    if python3 -c "from dags.news_sentiment_pipeline import dag; print('News pipeline DAG syntax OK')"; then
        success "News pipeline DAG syntax validation passed"
    else
        error "News pipeline DAG syntax validation failed"
    fi
    
    if python3 -c "from dags.movielens_analytics_pipeline import dag; print('MovieLens pipeline DAG syntax OK')"; then
        success "MovieLens pipeline DAG syntax validation passed"
    else
        error "MovieLens pipeline DAG syntax validation failed"
    fi
}

# Function to rollback if needed
rollback() {
    log "Rolling back deployment..."
    
    # Stop current services
    stop_services
    
    # Remove current deployment
    sudo rm -rf "$DEPLOYMENT_DIR"
    
    # Restore from backup
    if [ -d "$BACKUP_DIR-$TIMESTAMP" ]; then
        sudo cp -r "$BACKUP_DIR-$TIMESTAMP" "$DEPLOYMENT_DIR"
        sudo chown -R $USER:$USER "$DEPLOYMENT_DIR"
        success "Rollback completed"
    else
        error "No backup found for rollback"
    fi
    
    # Start services
    start_services
}

# Main deployment function
main() {
    log "Starting deployment process..."
    
    # Check permissions
    check_permissions
    
    # Create backup
    create_backup
    
    # Stop services
    stop_services
    
    # Deploy new code
    deploy_code
    
    # Start services
    start_services
    
    # Validate deployment
    validate_deployment
    
    # Run tests
    run_tests
    
    success "Deployment completed successfully!"
    log "Deployment log saved to: $LOG_FILE"
}

# Handle command line arguments
case "${1:-deploy}" in
    "deploy")
        main
        ;;
    "rollback")
        rollback
        ;;
    "validate")
        validate_deployment
        ;;
    "test")
        run_tests
        ;;
    *)
        echo "Usage: $0 {deploy|rollback|validate|test}"
        echo "  deploy   - Deploy new code (default)"
        echo "  rollback - Rollback to previous version"
        echo "  validate - Validate current deployment"
        echo "  test     - Run post-deployment tests"
        exit 1
        ;;
esac 