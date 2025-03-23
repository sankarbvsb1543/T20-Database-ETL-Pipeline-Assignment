import sys
import os
import json
import logging
from datetime import datetime

# Get the root directory (ETL Assignment) and add it to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# Import functions from the Functions package
from Functions.data_extraction import get_all_tables, extract_table_data, read_saved_files_config
from Functions.data_storage import save_dataframe
from Functions.data_loading import target_load
from Connections.db_connection import load_config, get_spark_session
from logging_config import logger  # Import logger from logging_config

def main():
    """Main function to extract, transform, and load data."""
    try:
        logger.info("Starting ETL pipeline...", extra={"operation": "start", "status": "success"})
        config = load_config()
        spark = get_spark_session()
        tables = get_all_tables(spark)
        storage_root = config.get("storage_root", "data/")

        # Load saved files configuration
        with open("saved_files_config.json", "r") as file:
            saved_files_config = json.load(file)

        if not tables:
            logger.warning("No tables found in the database.", extra={"operation": "fetch_tables", "status": "failure"})
            return

        # Extract and save data
        for table_name in tables:
            table_config = next((item for item in saved_files_config if item["table_name"] == table_name), None)
            if not table_config:
                logger.warning(f"No configuration found for table {table_name}. Skipping...", extra={"table_name": table_name, "operation": "config_check", "status": "failure"})
                continue

            formats = table_config["format"]
            storage_path = table_config["directory"]
            options = table_config.get("options", {})
            options = {k: v for k, v in options.items() if v is not None}

            if table_name == 'Match':
                logger.info(f"Processing {table_name} Random 50 rows...", extra={"table_name": table_name, "operation": "extract"})
                df = extract_table_data(spark, table_name, "source_database", True, 50)
                save_dataframe(df, table_name, formats, options, storage_path)
            elif table_name == 'BallbyBallActivity':
                full_load_path = os.path.join(storage_root, table_name + "_Full_Load")
                logger.info(f"Processing {table_name}_Full Load...", extra={"table_name": table_name, "operation": "extract"})
                df = extract_table_data(spark, table_name, "source_database")
                save_dataframe(df, table_name, formats, options, full_load_path)

                random_load_path = os.path.join(storage_root, table_name)
                logger.info(f"Processing {table_name} Random 10,000 rows...", extra={"table_name": table_name, "operation": "extract"})
                df = extract_table_data(spark, table_name, "source_database", True, 10000)
                save_dataframe(df, table_name, formats, options, random_load_path)
            else:
                logger.info(f"Processing {table_name}...", extra={"table_name": table_name, "operation": "extract"})
                df = extract_table_data(spark, table_name, "source_database")
                save_dataframe(df, table_name, formats, options, storage_path)

        # Read saved files and create Spark DataFrames
        logger.info("Reading saved files and creating DataFrames...", extra={"operation": "read_files", "status": "success"})
        dataframes = read_saved_files_config(spark)
        for table_name, df in dataframes.items():
            logger.info(f"DataFrame for {table_name}:", extra={"table_name": table_name, "operation": "create_dataframe", "status": "success"})
            df.printSchema()
            df.show(1)

        # Load data into the target database
        logger.info("Loading data into the target database...", extra={"operation": "load_data", "status": "success"})
        target_load(spark, dataframes)
        logger.info("ETL pipeline completed successfully.", extra={"operation": "end", "status": "success"})
        print("\n")
        print("Execution plans for all dataframes:\n")
        for df in dataframes.values():
            df.explain()
    except Exception as e:
        logger.error(f"Error in ETL pipeline: {e}", exc_info=True, extra={"operation": "end", "status": "failure", "error": str(e)})
        raise

if __name__ == "__main__":
    main()