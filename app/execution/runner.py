from app.db import DatabaseConnector
from app.anonymization import Anonymizer
from app.logging import get_audit_logger
from sqlalchemy import text, MetaData, Table, select, inspect
import sqlalchemy

class ExecutionEngine:
    def __init__(self, db: DatabaseConnector, anonymizer: Anonymizer):
        self.db = db
        self.anonymizer = anonymizer
        self.logger = get_audit_logger()

    def execute(self, sensitive_columns):
        # Group by table
        tables = {}
        for col in sensitive_columns:
            key = (col['schema'], col['table'])
            if key not in tables:
                tables[key] = []
            tables[key].append(col)

        with self.db.engine.connect() as conn:
            # Begin Transaction
            trans = conn.begin()
            print("Transaction started.")
            try:
                for (schema, table_name), cols in tables.items():
                    self._process_table(conn, schema, table_name, cols)

                trans.commit()
                print("Execution completed successfully. Changes committed.")
            except Exception as e:
                trans.rollback()
                print(f"Execution FAILED. Rolled back. Error: {e}")
                # Re-raise to alert caller
                raise

    def _process_table(self, conn, schema, table_name, cols):
        full_table = f"{schema}.{table_name}" if schema else table_name
        print(f"Processing table {full_table}...")

        pk_cols = self._get_pk(table_name, schema)
        if not pk_cols:
            print(f"Warning: No PK found for {full_table}. Updates require PK. Skipping individual row updates.")
            # Could implement bulk update here if needed, but risky without logging IDs.
            return

        t = Table(table_name, MetaData(), schema=schema, autoload_with=self.db.engine)

        # Select PKs + Sensitive Cols
        sel_pk = [t.c[pk] for pk in pk_cols]
        sel_cols = [t.c[c['column']] for c in cols]

        stmt = select(*(sel_pk + sel_cols))

        # Stream results to handle large tables
        proxy = conn.execution_options(stream_results=True).execute(stmt)

        count = 0
        for row in proxy:
            # Identify Row
            row_id_parts = [str(row[i]) for i in range(len(pk_cols))]
            row_id = "-".join(row_id_parts)

            pk_where = {}
            for i, pk in enumerate(pk_cols):
                pk_where[pk] = row[i]

            changes = {}
            offset = len(pk_cols)

            # Calculate changes
            for i, col_def in enumerate(cols):
                orig_val = row[offset + i]
                # Skip if already None? Or anonymize None? Usually None stays None.
                if orig_val is None:
                    continue

                fake_val = self.anonymizer.get_fake_value(orig_val, col_def['sensitive_type'])

                if str(fake_val) != str(orig_val):
                    changes[col_def['column']] = fake_val
                    # Log
                    self.logger.log_change(full_table, col_def['column'], row_id, orig_val, fake_val)

            if changes:
                # Execute Update for this row
                upd_stmt = t.update().values(**changes)
                for pk_col, pk_val in pk_where.items():
                    upd_stmt = upd_stmt.where(t.c[pk_col] == pk_val)

                conn.execute(upd_stmt)
                count += 1

        print(f"  Updated {count} rows in {full_table}.")

    def _get_pk(self, table, schema):
        try:
            insp = inspect(self.db.engine)
            return insp.get_pk_constraint(table, schema=schema)['constrained_columns']
        except:
            return []
