import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def create_spark_session(app_name="MovieLensProcessor"):
    """
    Create and configure a Spark session for MovieLens data processing
    
    Args:
        app_name (str): Name of the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    
    # Spark configuration
    conf = SparkConf()
    
    # Basic Spark configuration
    conf.set("spark.app.name", app_name)
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
    
    # Memory and performance tuning
    conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
    conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
    conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
    
    # Serialization
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer.max", "1024m")
    
    # Dynamic allocation
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.dynamicAllocation.minExecutors", "1")
    conf.set("spark.dynamicAllocation.maxExecutors", "4")
    conf.set("spark.dynamicAllocation.initialExecutors", "2")
    
    # Shuffle configuration
    conf.set("spark.sql.shuffle.partitions", "200")
    conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")
    
    # Memory fraction
    conf.set("spark.memory.fraction", "0.8")
    conf.set("spark.memory.storageFraction", "0.3")
    
    # Network timeout
    conf.set("spark.network.timeout", "800s")
    conf.set("spark.executor.heartbeatInterval", "60s")
    
    # Create Spark session
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def get_spark_master_url():
    """
    Get the Spark master URL from environment variables
    
    Returns:
        str: Spark master URL
    """
    return os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077')

def configure_spark_for_airflow():
    """
    Configure Spark session specifically for Airflow environment
    
    Returns:
        SparkSession: Configured Spark session for Airflow
    """
    
    # Check if we're in a cluster environment or local development
    # Use local mode for local development, cluster mode for production
    is_local = False
    
    if is_local:
        # Use local mode for development
        master_url = "local[*]"
        
        # Add JAR files to classpath for local development
        # These JAR files are needed for S3 connectivity
        current_dir = os.getcwd()
        jar_files = [
            os.path.join(current_dir, "spark-jars", "hadoop-aws-3.3.4.jar"),
            os.path.join(current_dir, "spark-jars", "aws-java-sdk-bundle-1.12.261.jar")
        ]
        
        # Filter out non-existent JAR files
        existing_jars = [jar for jar in jar_files if os.path.exists(jar)]
        
        if existing_jars:
            # Build the classpath string with absolute paths
            classpath = ":".join(existing_jars)
            
            # Create Spark session with JAR files for local development
            spark = SparkSession.builder \
                .appName("MovieLensAirflowProcessor") \
                .master(master_url) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.shuffle.partitions", "200") \
                .config("spark.dynamicAllocation.enabled", "true") \
                .config("spark.dynamicAllocation.minExecutors", "1") \
                .config("spark.dynamicAllocation.maxExecutors", "4") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.network.timeout", "800s") \
                .config("spark.executor.heartbeatInterval", "60s") \
                .config("spark.driver.extraClassPath", classpath) \
                .config("spark.executor.extraClassPath", classpath) \
                .getOrCreate()
        else:
            # Create Spark session without JAR files if they don't exist
            print("Warning: JAR files not found. S3 connectivity may not work.")
            spark = SparkSession.builder \
                .appName("MovieLensAirflowProcessor") \
                .master(master_url) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.shuffle.partitions", "200") \
                .config("spark.dynamicAllocation.enabled", "true") \
                .config("spark.dynamicAllocation.minExecutors", "1") \
                .config("spark.dynamicAllocation.maxExecutors", "4") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.network.timeout", "800s") \
                .config("spark.executor.heartbeatInterval", "60s") \
                .getOrCreate()
    else:
        # Use cluster mode for production
        master_url = get_spark_master_url()
        
        # Create Spark session for cluster mode
        spark = SparkSession.builder \
            .appName("MovieLensAirflowProcessor") \
            .master(master_url) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.dynamicAllocation.minExecutors", "1") \
            .config("spark.dynamicAllocation.maxExecutors", "4") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.network.timeout", "800s") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def stop_spark_session(spark_session):
    """
    Safely stop a Spark session
    
    Args:
        spark_session (SparkSession): Spark session to stop
    """
    if spark_session:
        try:
            spark_session.stop()
        except Exception as e:
            print(f"Warning: Error stopping Spark session: {e}")

# Spark configuration constants
SPARK_CONFIG = {
    'app_name': 'MovieLensDataProcessor',
    'master_url': 'spark://spark-master:7077',
    'shuffle_partitions': 200,
    'memory_fraction': 0.8,
    'storage_fraction': 0.3,
    'network_timeout': '800s',
    'heartbeat_interval': '60s'
} 