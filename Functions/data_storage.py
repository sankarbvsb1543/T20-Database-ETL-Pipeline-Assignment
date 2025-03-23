import os
import pandas as pd
import json
import logging
from datetime import datetime
from Code.logging_config import logger  # Import logger from logging_config

def save_dataframe(df, table_name, format, options, storage_root):
    """Save Spark DataFrame as Pandas to multiple formats."""
    try:
        if df is None:
            logger.warning(f"Skipping {table_name}: No data available.", extra={"table_name": table_name, "operation": "save", "status": "failure"})
            return

        table_path = os.path.join(storage_root)
        os.makedirs(table_path, exist_ok=True)

        # Convert Spark DataFrame to Pandas
        pdf = df.toPandas()
        if format == "csv" and options.get("sep") == "\t":
            format = "tsv"
        file_name = f"{table_name}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.{format}"
        file_path = os.path.join(table_path, file_name)

        if format == "csv":
            pdf.to_csv(file_path, index=False)
        elif format == "parquet":
            pdf.to_parquet(file_path, index=False, engine='fastparquet')
        elif format == "json":
            pdf.to_json(file_path, orient="records", lines=True)
        elif format == "tsv":
            pdf.to_csv(file_path, sep='\t', index=False)
        elif format == "xlsx":
            pdf.to_excel(file_path, index=False)
        else:
            raise ValueError(f"Unsupported format: {format}")

        logger.info(f"Saved {table_name} as {format} in {file_path}", extra={"table_name": table_name, "operation": "save", "status": "success", "records_processed": len(pdf)})
    except Exception as e:
        logger.error(f"Error saving {table_name} in {format}: {e}", exc_info=True, extra={"table_name": table_name, "operation": "save", "status": "failure", "error": str(e)})