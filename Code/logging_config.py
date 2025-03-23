import logging
import json
from datetime import datetime

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
            "exception": record.exc_info,
            "status": getattr(record, "status", None),  # Add status field
            "table_name": getattr(record, "table_name", None),  # Add table_name field
            "operation": getattr(record, "operation", None),  # Add operation field
            "records_processed": getattr(record, "records_processed", None),  # Add records_processed field
            "error": getattr(record, "error", None)  # Add error field
        }
        return json.dumps(log_record)

# Configure logging
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
file_handler = logging.FileHandler("etl_pipeline.log")
formatter = JsonFormatter()
handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
logger.addHandler(handler)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)