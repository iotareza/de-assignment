FROM apache/airflow:2.7.1-python3.9

USER root

# Install system dependencies including Java for Spark and MinIO client
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    wget \
    git \
    libpq-dev \
    procps \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable for OpenJDK 17 (arm64)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$PATH:$JAVA_HOME/bin

# Set working directory
WORKDIR ${AIRFLOW_HOME}

USER airflow

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Create necessary directories
RUN mkdir -p ${AIRFLOW_HOME}/dags \
    && mkdir -p ${AIRFLOW_HOME}/logs \
    && mkdir -p ${AIRFLOW_HOME}/plugins \
    && mkdir -p ${AIRFLOW_HOME}/data/temp

# Copy application code
# COPY src/ ./src/
# COPY dags/ ./dags/

# Health check
HEALTHCHECK CMD ["curl", "--fail", "http://localhost:8080/health"]

# Default command
CMD ["webserver"]
# CMD ["tail", "-f", "/dev/null"]