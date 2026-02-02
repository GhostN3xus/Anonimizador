import logging
import sys
import os
import datetime
from app.config import Config

_audit_logger_instance = None

def setup_logging():
    logging.basicConfig(
        level=Config.LOG_LEVEL,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("app.log")
        ]
    )

class AuditLogger:
    def __init__(self):
        self.logger = logging.getLogger("AUDIT")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        formatter = logging.Formatter('%(asctime)s | %(message)s')

        # File handler for human readable audit
        fh = logging.FileHandler('audit.log')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        # Rollback logger for data recovery
        self.rollback_logger = logging.getLogger("ROLLBACK")
        self.rollback_logger.setLevel(logging.INFO)
        self.rollback_logger.propagate = False
        rh = logging.FileHandler('rollback.csv')
        # Simple CSV format for rollback
        rh.setFormatter(logging.Formatter('%(message)s'))
        self.rollback_logger.addHandler(rh)

        # Write header to rollback file if empty
        if os.stat('rollback.csv').st_size == 0:
            self.rollback_logger.info("timestamp|table|column|row_id|original_value|new_value")

    def log_change(self, table, column, row_id, original_value, new_value):
        masked = self._mask(str(original_value))

        # Human readable log
        msg = f"TABLE: {table:<15} | COL: {column:<15} | ID: {str(row_id):<10} | ORIG: {masked:<20} | NEW: {str(new_value)}"
        self.logger.info(msg)

        # Rollback log - pipe separated for parsing
        ts = datetime.datetime.now().isoformat()
        # Escape pipes in values
        orig_safe = str(original_value).replace('|', '\\|').replace('\n', '\\n')
        new_safe = str(new_value).replace('|', '\\|').replace('\n', '\\n')

        r_msg = f"{ts}|{table}|{column}|{row_id}|{orig_safe}|{new_safe}"
        self.rollback_logger.info(r_msg)

    def _mask(self, val):
        s = str(val)
        if len(s) <= 4:
            return '*' * len(s)
        return s[:2] + '*' * (len(s)-4) + s[-2:]

def get_audit_logger():
    global _audit_logger_instance
    if _audit_logger_instance is None:
        _audit_logger_instance = AuditLogger()
    return _audit_logger_instance
