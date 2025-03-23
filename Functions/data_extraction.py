import os
import json
import logging
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, DateType, TimestampType
from Connections.db_connection import load_config

logger = logging.getLogger(__name__)

def get_all_tables(spark):
    """Fetch all table names from INFORMATION_SCHEMA.TABLES except 'sysdiagrams'."""
    try:
        logger.info("Fetching all table names from the source database...", extra={"operation": "fetch_tables", "status": "start"})
        config = load_config()
        db_config = config["source_database"]
        query = "(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME <> 'sysdiagrams' AND TABLE_NAME NOT LIKE '%ScoreCard') AS table_list"
        table_df = spark.read \
            .format("jdbc") \
            .option("url", db_config["url"]) \
            .option("dbtable", query) \
            .option("user", db_config["user"]) \
            .option("password", db_config["password"]) \
            .option("driver", db_config["driver"]) \
            .load()
        tables = [row["TABLE_NAME"] for row in table_df.collect()]
        logger.info(f"Found {len(tables)} tables: {tables}", extra={"operation": "fetch_tables", "status": "success", "records_processed": len(tables)})
        return tables
    except Exception as e:
        logger.error(f"Error fetching table names: {e}", exc_info=True, extra={"operation": "fetch_tables", "status": "failure", "error": str(e)})
        return []

def extract_table_data(spark, table_name, database_type, incremental_flag=False, top_n=0):
    """Extract data from a table and return as DataFrame."""
    try:
        if incremental_flag:
            logger.info(f"Extracting random {top_n} rows from {table_name}...", extra={"table_name": table_name, "operation": "extract", "status": "start"})
            query = f"(SELECT TOP {top_n} * FROM {table_name} ORDER BY NEWID()) AS temp"
        else:
            logger.info(f"Extracting full data from {table_name}...", extra={"table_name": table_name, "operation": "extract", "status": "start"})
            query = table_name  # Fetch all rows

        config = load_config()
        db_config = config[database_type]
        df = spark.read \
            .format("jdbc") \
            .option("url", db_config["url"]) \
            .option("dbtable", query) \
            .option("user", db_config["user"]) \
            .option("password", db_config["password"]) \
            .option("driver", db_config["driver"]) \
            .load()
        logger.info(f"Extracted {df.count()} rows from {table_name}", extra={"table_name": table_name, "operation": "extract", "status": "success", "records_processed": df.count()})
        return df
    except Exception as e:
        logger.error(f"Error reading table {table_name}: {e}", exc_info=True, extra={"table_name": table_name, "operation": "extract", "status": "failure", "error": str(e)})
        return None

def get_spark_data_type(data_type):
    """Map string data type to Spark data type."""
    if data_type == "string":
        return StringType()
    elif data_type == "int":
        return IntegerType()
    elif data_type == "bigint":
        return LongType()
    elif data_type == "double":
        return DoubleType()
    elif data_type == "float":
        return FloatType()
    elif data_type == "boolean":
        return BooleanType()
    elif data_type == "date":
        return DateType()
    elif data_type == "timestamp":
        return TimestampType()
    else:
        return StringType()

def get_latest_file(directory, file_format):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(f".{file_format}")]
    if not files:
        raise FileNotFoundError(f"No files found in directory: {directory} with format: {file_format}")
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def read_saved_files_config(spark):
    """Read the saved_files_config.json and create Spark DataFrames."""
    config_file = "saved_files_config.json"
    if not os.path.exists(config_file) or os.path.getsize(config_file) == 0:
        raise FileNotFoundError(f"Config file not found or is empty at: {config_file}")

    with open(config_file, "r") as file:
        try:
            config_data = json.load(file)
        except json.JSONDecodeError:
            raise ValueError(f"Config file is empty or corrupted: {config_file}")

    dataframes = {}

    for entry in config_data:
        table_name = entry["table_name"]
        file_format = entry["format"]
        directory = entry["directory"]
        options = entry.get("options", {})
        data_types = entry.get("data_types", {})

        # Remove None values from options
        options = {k: v for k, v in options.items() if v is not None}

        # Create schema from data_types
        schema = StructType([StructField(name, get_spark_data_type(dtype), True) for name, dtype in data_types.items()])
        try:
            # Check if the file format is csv and the separator is \t, then search for .tsv files
            if file_format == "csv" and options.get("sep") == "\t":
                file_format = "tsv"

            # Get the latest file from the directory
            latest_file = get_latest_file(directory, file_format)

            # Read the file with schema inference
            if file_format == "tsv":
                df = spark.read.option("delimiter", "\t").csv(latest_file, header=True, schema=schema)
            else:
                df = spark.read.format(file_format).options(**options).load(latest_file)

            # Type cast columns according to the data types specified in the configuration file
            for column, dtype in data_types.items():
                df = df.withColumn(column, df[column].cast(get_spark_data_type(dtype)))

            dataframes[table_name] = df
            logger.info(f"Read saved file for {table_name}", extra={"table_name": table_name, "operation": "read_file", "status": "success", "records_processed": df.count()})
        except FileNotFoundError as e:
            logger.warning(f"Skipping {table_name}: {e}", extra={"table_name": table_name, "operation": "read_file", "status": "failure", "error": str(e)})

    return dataframes