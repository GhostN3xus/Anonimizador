from app.db import DatabaseConnector
from app.anonymization import Anonymizer
from sqlalchemy import text, select, Table, MetaData

class SimulationEngine:
    def __init__(self, db: DatabaseConnector, anonymizer: Anonymizer):
        self.db = db
        self.anonymizer = anonymizer

    def simulate(self, sensitive_columns):
        """
        Generates a preview of changes.
        """
        impact_report = []

        # Group by table to optimize queries
        tables = {}
        for col in sensitive_columns:
            key = (col['schema'], col['table'])
            if key not in tables:
                tables[key] = []
            tables[key].append(col)

        print("\n--- SIMULATION PREVIEW (First 2 rows per table) ---")

        for (schema, table_name), cols in tables.items():
            full_table = f"{schema}.{table_name}" if schema else table_name
            print(f"\nTABLE: {full_table}")

            try:
                with self.db.engine.connect() as conn:
                    # Use reflection to handle quoting safely
                    t = Table(table_name, MetaData(), schema=schema, autoload_with=self.db.engine)

                    # Select columns we care about
                    selected_columns = [t.c[c['column']] for c in cols]

                    # Limit 2
                    stmt = select(*selected_columns).limit(2)
                    result = conn.execute(stmt).fetchall()

                    for i, row in enumerate(result):
                        print(f"  ROW {i+1}:")
                        for idx, val in enumerate(row):
                            col_def = cols[idx] # Order corresponds to selected_columns
                            col_name = col_def['column']
                            sens_type = col_def['sensitive_type']

                            fake = self.anonymizer.get_fake_value(val, sens_type)
                            print(f"    {col_name:<15}: {str(val):<20} -> {fake} ({sens_type})")

                            impact_report.append({
                                'table': full_table,
                                'column': col_name,
                                'original': val,
                                'new': fake
                            })
            except Exception as e:
                print(f"    Error simulating table {full_table}: {e}")

        return impact_report
