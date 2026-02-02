from app.db import DatabaseConnector
from app.ml import SensitiveDataClassifier
import logging

class SensitiveDiscovery:
    def __init__(self, db_connector: DatabaseConnector):
        self.db = db_connector
        self.classifier = SensitiveDataClassifier()
        self.logger = logging.getLogger("SensitiveDiscovery")

    def scan(self):
        """
        Scans the database for sensitive columns.
        Returns a list of dictionaries describing sensitive columns.
        """
        sensitive_columns = []
        tables = self.db.get_tables()

        print(f"Starting scan on {len(tables)} tables...")

        for schema, table in tables:
            full_table_name = f"{schema}.{table}" if schema else table
            # Check empty
            if self.db.is_table_empty(table, schema):
                self.logger.info(f"Skipping empty table {full_table_name}")
                continue

            columns = self.db.get_columns(table, schema)
            for col in columns:
                col_name = col['name']

                # Skip if column type is obviously not text-like?
                # ML model handles int/dates too, so we pass everything generally.
                # But maybe skip boolean?

                samples = self.db.sample_data(table, col_name, schema, limit=50)
                if not samples:
                    continue

                stats = self.db.get_column_stats(table, col_name, schema)
                sql_type_obj = col['type']
                sql_type_str = str(sql_type_obj)
                max_size = getattr(sql_type_obj, 'length', 0) or 0

                # Predict
                label, confidence = self.classifier.predict_column(samples, col_name, sql_type_str, stats, max_size)

                if label != 'NON_SENSITIVE':
                    self.logger.info(f"Detected {label} in {full_table_name}.{col_name} (Conf: {confidence:.2f})")
                    sensitive_columns.append({
                        'schema': schema,
                        'table': table,
                        'column': col_name,
                        'current_type': sql_type,
                        'sensitive_type': label,
                        'confidence': confidence,
                        'sample_value': samples[0] if samples else ""
                    })

        return sensitive_columns
