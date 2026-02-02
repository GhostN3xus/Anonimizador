import sqlalchemy
from sqlalchemy import create_engine, inspect, text, select
from sqlalchemy.schema import MetaData, Table
from app.config import Config
import logging

class DatabaseConnector:
    def __init__(self):
        self.engine = None
        self.inspector = None
        self.logger = logging.getLogger("DatabaseConnector")
        self.metadata = MetaData()

    def connect(self):
        try:
            self.engine = create_engine(Config.DB_CONNECTION_STRING)
            # Test connection
            with self.engine.connect() as conn:
                pass
            self.inspector = inspect(self.engine)
            self.logger.info("Connected to database.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise

    def get_tables(self):
        """Returns list of (schema, table_name)"""
        tables_list = []
        try:
            # For SQLite, schema is None
            if self.engine.name == 'sqlite':
                for table_name in self.inspector.get_table_names():
                    tables_list.append((None, table_name))
            else:
                schemas = self.inspector.get_schema_names()
                # Filter out system schemas usually?
                # For now, we list all, user can filter if needed or we handle permissions errors
                for schema in schemas:
                    try:
                        tables = self.inspector.get_table_names(schema=schema)
                        for t in tables:
                            tables_list.append((schema, t))
                    except Exception as e:
                        self.logger.debug(f"Could not access schema {schema}: {e}")
        except Exception as e:
            self.logger.error(f"Error listing tables: {e}")

        return tables_list

    def get_columns(self, table_name, schema=None):
        try:
            return self.inspector.get_columns(table_name, schema=schema)
        except Exception as e:
            self.logger.error(f"Error getting columns for {schema}.{table_name}: {e}")
            return []

    def is_table_empty(self, table_name, schema=None):
        full_name = f"{schema}.{table_name}" if schema else table_name
        # Simple count query
        # Quoting table name is important if it has spaces or special chars
        # We'll use SQLAlchemy text() but might need manual quoting for the string part if not using Table object
        # Safest is to try/except with a simple query first

        try:
            with self.engine.connect() as conn:
                # Use text with simple quoting logic or just let user beware?
                # Best to use Table reflection to construct query safely
                # But creating Table object for every check might be slow?
                # Let's try raw SQL for speed on check, falling back to reflection if needed.
                # Actually, `count(*)` is standard.

                # To support spaces in table names:
                if schema:
                    quoted_name = f'"{schema}"."{table_name}"' if self.engine.name != 'mssql' else f'[{schema}].[{table_name}]'
                else:
                    quoted_name = f'"{table_name}"' if self.engine.name != 'mssql' else f'[{table_name}]'

                query = text(f"SELECT COUNT(*) FROM {quoted_name}")
                result = conn.execute(query).scalar()
                return result == 0
        except Exception as e:
            self.logger.warning(f"Could not check emptiness for {table_name} (using reflection fallback): {e}")
            # Fallback to reflection
            try:
                with self.engine.connect() as conn:
                    t = Table(table_name, MetaData(), schema=schema, autoload_with=self.engine)
                    query = select(sqlalchemy.func.count()).select_from(t)
                    result = conn.execute(query).scalar()
                    return result == 0
            except Exception as e2:
                self.logger.error(f"Reflection fallback failed for {table_name}: {e2}")
                return True # Treat as empty/unusable

    def sample_data(self, table_name, column_name, schema=None, limit=100):
        try:
            with self.engine.connect() as conn:
                t = Table(table_name, MetaData(), schema=schema, autoload_with=self.engine)
                # Select distinct non-null values to get better variety for ML
                stmt = select(t.c[column_name]).where(t.c[column_name].is_not(None)).distinct().limit(limit)
                result = conn.execute(stmt).scalars().all()
                return [str(r) for r in result]
        except Exception as e:
            self.logger.error(f"Error sampling data from {table_name}.{column_name}: {e}")
            return []

    def get_column_stats(self, table_name, column_name, schema=None):
        """Returns basic stats: null_percentage, unique_ratio"""
        try:
            with self.engine.connect() as conn:
                t = Table(table_name, MetaData(), schema=schema, autoload_with=self.engine)
                col = t.c[column_name]

                count_query = select(sqlalchemy.func.count()).select_from(t)
                total_rows = conn.execute(count_query).scalar()

                if total_rows == 0:
                    return {'null_percentage': 1.0, 'unique_ratio': 0.0, 'total_rows': 0}

                null_query = select(sqlalchemy.func.count()).where(col.is_(None)).select_from(t)
                null_rows = conn.execute(null_query).scalar()

                # For unique count, strictly it can be expensive on large tables.
                # Maybe skip for massive tables? Or use sample?
                # Requirements: "Percentual de valores únicos... Percentual de NULLs"
                # We will try to get it.
                distinct_query = select(sqlalchemy.func.count(sqlalchemy.func.distinct(col))).select_from(t)
                unique_rows = conn.execute(distinct_query).scalar()

                return {
                    'null_percentage': null_rows / total_rows,
                    'unique_ratio': unique_rows / total_rows,
                    'total_rows': total_rows
                }
        except Exception as e:
            self.logger.warning(f"Could not get stats for {table_name}.{column_name}: {e}")
            return {'null_percentage': 0, 'unique_ratio': 0, 'total_rows': 0}

    def close(self):
        if self.engine:
            self.engine.dispose()
