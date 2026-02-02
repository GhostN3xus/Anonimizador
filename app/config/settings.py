import os
import sys
from dotenv import load_dotenv

load_dotenv()

class Config:
    DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
    ANONYMIZATION_DB_PATH = os.getenv('ANONYMIZATION_DB_PATH', 'anonymization_mapping.db')

    # ML Model Path
    ML_MODEL_PATH = os.getenv('ML_MODEL_PATH', 'app/ml/trained_model.pkl')

    @classmethod
    def validate(cls):
        if not cls.DB_CONNECTION_STRING:
            print("WARNING: DB_CONNECTION_STRING is not set in .env or environment.")
            print("Please set it to a valid SQLAlchemy connection string.")
            print("Example for SQL Server: mssql+pyodbc://user:password@server/db?driver=ODBC+Driver+17+for+SQL+Server")
            print("Example for SQLite: sqlite:///example.db")
            # We don't raise error immediately to allow importing Config in tests without env
            return False
        return True
