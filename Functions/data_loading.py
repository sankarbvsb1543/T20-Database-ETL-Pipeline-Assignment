import os
import json
import logging
import pyodbc
from pyspark.sql.functions import col, lit, current_date, sha2, concat_ws
from Connections.db_connection import load_config
from Functions.data_extraction import extract_table_data

logger = logging.getLogger(__name__)

def target_load(spark, dataframes):
    """Load the transformed data into the target database."""
    try:
        logger.info("Starting data loading process...", extra={"operation": "load_data", "status": "start"})
        config_file = "saved_files_config.json"
        if not os.path.exists(config_file) or os.path.getsize(config_file) == 0:
            logger.error(f"Config file not found or is empty at: {config_file}", extra={"operation": "load_data", "status": "failure"})
            raise FileNotFoundError(f"Config file not found or is empty at: {config_file}")

        with open(config_file, "r") as file:
            try:
                config_data = json.load(file)
            except json.JSONDecodeError:
                logger.error(f"Config file is empty or corrupted: {config_file}", extra={"operation": "load_data", "status": "failure"})
                raise ValueError(f"Config file is empty or corrupted: {config_file}")

        config = load_config()
        db_config = config["target_database"]

        for entry in config_data:
            table_name = entry["table_name"]
            loading_type = entry["loading"]
            source_df = dataframes[table_name]
            unique_key_columns = entry.get("unique_key_columns", [])

            if table_name == 'BallbyBallActivity' and loading_type == "incremental load":
                logger.info(f"Handling SCD Type 1 for {table_name}...", extra={"table_name": table_name, "operation": "scd_type_1", "status": "start"})
                handle_scd_type_1(spark, db_config, table_name, source_df, unique_key_columns)
            elif table_name == 'Match' and loading_type == "incremental load":
                logger.info(f"Handling SCD Type 2 for {table_name}...", extra={"table_name": table_name, "operation": "scd_type_2", "status": "start"})
                handle_scd_type_2(spark, db_config, table_name, source_df, unique_key_columns)
            elif loading_type == "truncate load":
                logger.info(f"Handling truncate load for {table_name}...", extra={"table_name": table_name, "operation": "truncate_load", "status": "start"})
                handle_truncate_load(db_config, table_name, source_df)
            else:
                logger.warning(f"Unsupported loading type for table {table_name}: {loading_type}", extra={"table_name": table_name, "operation": "load_data", "status": "failure"})

        logger.info("Data loading process completed successfully.", extra={"operation": "load_data", "status": "success"})
    except Exception as e:
        logger.error(f"Error in data loading process: {e}", exc_info=True, extra={"operation": "load_data", "status": "failure", "error": str(e)})
        raise

def handle_scd_type_1(spark, db_config, table_name, source_df, unique_key_columns):
    """Handle SCD Type 1 for BallbyBallActivity."""
    try:
        target_df = extract_table_data(spark, table_name, "target_database")

        # Add hash column for comparison
        target_df = target_df.withColumn("hash", sha2(concat_ws("", *target_df.columns), 256))
        source_df = source_df.withColumn("hash", sha2(concat_ws("", *source_df.columns), 256))

        # Identify new records
        new_records_df = source_df.join(target_df, unique_key_columns, "left_anti")

        # Identify updated records
        updated_records_df = source_df.join(target_df, unique_key_columns, "inner") \
            .filter(source_df["hash"] != target_df["hash"])

        server = 'PTDELL0315'
        database = 'T20_Target'
        username = 'SA'
        password = 'Welcome@1234'  
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # Append new records
        if not new_records_df.rdd.isEmpty():
            new_records_df.drop("hash").write \
                .format("jdbc") \
                .option("url", db_config["url"]) \
                .option("dbtable", table_name) \
                .option("user", db_config["user"]) \
                .option("password", db_config["password"]) \
                .option("driver", db_config["driver"]) \
                .mode("append") \
                .save()
            logger.info(f"Appended new records to {table_name}", extra={"table_name": table_name, "operation": "scd_type_1", "status": "success", "records_processed": new_records_df.count()})

        # Delete and insert updated records
        if not updated_records_df.rdd.isEmpty():
            # Delete existing rows that match the updated records
            delete_sql = f"""
                DELETE FROM {table_name}
                WHERE {" AND ".join([f"{col} = ?" for col in unique_key_columns])}
            """
            delete_params = updated_records_df.select(*unique_key_columns).collect()
            for params in delete_params:
                cursor.execute(delete_sql, [getattr(params, col) for col in unique_key_columns])
            conn.commit()

            # Insert updated records
            updated_records_df.drop("hash").write \
                .format("jdbc") \
                .option("url", db_config["url"]) \
                .option("dbtable", table_name) \
                .option("user", db_config["user"]) \
                .option("password", db_config["password"]) \
                .option("driver", db_config["driver"]) \
                .mode("append") \
                .save()
            logger.info(f"Updated records in {table_name}", extra={"table_name": table_name, "operation": "scd_type_1", "status": "success", "records_processed": updated_records_df.count()})

        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error in SCD Type 1 for {table_name}: {e}", exc_info=True, extra={"table_name": table_name, "operation": "scd_type_1", "status": "failure", "error": str(e)})
        raise

def handle_scd_type_2(spark, db_config, table_name, source_df, unique_key_columns):
    """Handle SCD Type 2 for Match."""
    try:
        target_df = extract_table_data(spark, table_name, "target_database")

        # Check if the target table has the required SCD Type 2 columns
        required_columns = ["ValidFrom", "ValidTo", "IsCurrent"]
        missing_columns = [col for col in required_columns if col not in target_df.columns]
        if missing_columns:
            logger.error(f"Target table {table_name} is missing required SCD Type 2 columns: {missing_columns}", extra={"table_name": table_name, "operation": "scd_type_2", "status": "failure"})
            raise ValueError(f"Target table {table_name} is missing required SCD Type 2 columns: {missing_columns}")

        # Add SCD Type 2 columns to source DataFrame
        source_df = source_df.withColumn("ValidFrom", current_date())
        source_df = source_df.withColumn("ValidTo", lit(None).cast("date"))
        source_df = source_df.withColumn("IsCurrent", lit(1))

        # Identify new records
        new_records_df = source_df.join(target_df, unique_key_columns, "left_anti")

        # Identify updated records
        # updated_records_df = source_df.join(target_df, unique_key_columns, "inner") \
        #     .filter(source_df["hash"] != target_df["hash"])

        updated_records_df = source_df.alias("df").join(target_df.alias("target"), on="MatchID", how="inner") \
                                 .select("df.*")

        server = 'PTDELL0315'
        database = 'T20_Target'
        username = 'SA'
        password = 'Welcome@1234'  
        conn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()

        # Mark old records as not current
        if not updated_records_df.rdd.isEmpty():
            updated_records_keys_df = updated_records_df.select(*unique_key_columns).distinct()
            target_df_to_update = target_df.join(updated_records_keys_df, unique_key_columns, "inner") \
                .filter(target_df["IsCurrent"] == 1) \
                .withColumn("ValidTo", current_date()) \
                .withColumn("IsCurrent", lit(0))

            # Update the old records in the target table
            update_query = f"""
                UPDATE {table_name}
                SET ValidTo = ?, IsCurrent = 0
                WHERE {" AND ".join([f"{col} = ?" for col in unique_key_columns])}
            """
            update_params = target_df_to_update.select("ValidTo", *unique_key_columns).collect()
            for params in update_params:
                valid_to_str = params.ValidTo.strftime('%Y-%m-%d')  # Convert to string
                cursor.execute(update_query, [valid_to_str] + [getattr(params, col) for col in unique_key_columns])
            conn.commit()

        # Append new records
        if not new_records_df.rdd.isEmpty():
            new_records_df.drop("hash").write \
                .format("jdbc") \
                .option("url", db_config["url"]) \
                .option("dbtable", table_name) \
                .option("user", db_config["user"]) \
                .option("password", db_config["password"]) \
                .option("driver", db_config["driver"]) \
                .mode("append") \
                .save()
            logger.info(f"Appended new records to {table_name}", extra={"table_name": table_name, "operation": "scd_type_2", "status": "success", "records_processed": new_records_df.count()})

        # Append updated records as new rows
        if not updated_records_df.rdd.isEmpty():
            updated_records_df.drop("hash").write \
                .format("jdbc") \
                .option("url", db_config["url"]) \
                .option("dbtable", table_name) \
                .option("user", db_config["user"]) \
                .option("password", db_config["password"]) \
                .option("driver", db_config["driver"]) \
                .mode("append") \
                .save()
            logger.info(f"Appended updated records to {table_name}", extra={"table_name": table_name, "operation": "scd_type_2", "status": "success", "records_processed": updated_records_df.count()})

        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error in SCD Type 2 for {table_name}: {e}", exc_info=True, extra={"table_name": table_name, "operation": "scd_type_2", "status": "failure", "error": str(e)})
        raise

def handle_truncate_load(db_config, table_name, source_df):
    """Handle truncate and load for tables."""
    try:
        source_df.write \
            .format("jdbc") \
            .option("url", db_config["url"]) \
            .option("dbtable", table_name) \
            .option("user", db_config["user"]) \
            .option("password", db_config["password"]) \
            .option("driver", db_config["driver"]) \
            .mode("overwrite") \
            .save()
        logger.info(f"Truncate loaded data for {table_name}", extra={"table_name": table_name, "operation": "truncate_load", "status": "success", "records_processed": source_df.count()})
    except Exception as e:
        logger.error(f"Error in truncate load for {table_name}: {e}", exc_info=True, extra={"table_name": table_name, "operation": "truncate_load", "status": "failure", "error": str(e)})
        raise