import json
import os
import logging
from pyspark.sql import SparkSession
from Code.logging_config import logger  # Import logger from logging_config

def get_spark_session():
    """Create and return a Spark session."""
    try:
        logger.info("Creating Spark session...", extra={"operation": "create_spark_session", "status": "start"})
        
        # Set the correct Python executable
        os.environ['PYSPARK_PYTHON'] = 'C:/Users/sankar.burra/AppData/Local/Programs/Python/Python38/python.exe'
        os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/sankar.burra/AppData/Local/Programs/Python/Python38/python.exe'

        spark = SparkSession.builder \
            .appName("ETL Preparation") \
            .config("spark.driver.extraClassPath", "C:/Users/sankar.burra/AppData/Local/Programs/Python/Python38/Lib/site-packages/pyspark/jars/mssql-jdbc-12.8.1.jre8.jar") \
            .config("spark.python.worker.timeout", "600") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.cores", "2") \
            .config("spark.executor.heartbeatInterval", "60s") \
            .config("spark.network.timeout", "120s") \
            .config("spark.python.worker.reuse", "true") \
            .getOrCreate()
        
        if spark:
            logger.info("Created spark session successfully!", extra={"operation": "create_spark_session", "status": "success"})
        else:
            logger.error("Error in creating spark session", extra={"operation": "create_spark_session", "status": "failure"})
        
        spark.sparkContext.setLogLevel("ERROR")  # Reduce log verbosity
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}", exc_info=True, extra={"operation": "create_spark_session", "status": "failure", "error": str(e)})
        raise

def load_config():
    """Load and return the configuration from the config.json file."""
    try:
        logger.info("Loading configuration...", extra={"operation": "load_config", "status": "start"})
        
        script_dir = os.path.dirname(os.path.abspath(__file__))  # Get script directory
        project_root = os.path.abspath(os.path.join(script_dir, ".."))  # Go to project root
        config_path = os.path.join(project_root, "config.json")  # Construct full path
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found at: {config_path}")
        
        with open(config_path, "r") as file:
            config = json.load(file)
        
        logger.info("Configuration loaded.", extra={"operation": "load_config", "status": "success"})
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}", exc_info=True, extra={"operation": "load_config", "status": "failure", "error": str(e)})
        raise