# Sensitive Data Anonymizer

A 100% functional Python application for intelligent detection and correlated anonymization of sensitive data in SQL databases via ODBC. Designed to comply with **LGPD** and **PCI DSS** standards.

## Features

- **Modular Architecture**: Clean separation of concerns (Discovery, ML, Anonymization, Execution).
- **Intelligent Detection**: Hybrid approach using Regex and Self-Training Machine Learning (Logistic Regression).
- **Consistent Anonymization**: Maintains referential integrity and consistency (same original entity = same fake entity) across the database.
- **Audit & Rollback**: Generates human-readable audit logs and machine-readable rollback files.
- **Human Control**: Explicit validation steps for scope, mapping logic, and final execution.

## Project Structure

```
app/
 ├── config/          # Configuration and validation
 ├── db/              # ODBC connection and introspection
 ├── discovery/       # Sensitive data scanning
 ├── ml/              # Machine Learning model
 ├── anonymization/   # Fake data generation & mapping storage
 ├── simulation/      # Impact preview
 ├── execution/       # Transactional updates
 ├── logging/         # Audit logging
 └── main.py          # Orchestrator
```

## Prerequisites

- Python 3.8+
- ODBC Driver (if using SQL Server)

## Installation

1. Clone the repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Create a `.env` file in the root directory:

```ini
# Database Connection (SQLAlchemy format)
# Example for SQL Server:
DB_CONNECTION_STRING=mssql+pyodbc://user:password@server/database?driver=ODBC+Driver+17+for+SQL+Server

# Example for SQLite (for testing):
DB_CONNECTION_STRING=sqlite:///test.db

# Logging
LOG_LEVEL=INFO

# Paths
ANONYMIZATION_DB_PATH=anonymization_mapping.db
```

## Usage

Run the application module:

```bash
python -m app.main
```

### Workflow

1.  **Connection**: Connects to the target database.
2.  **Discovery**: Scans tables and identifies sensitive columns (Email, CPF, Name, Credit Card, etc.).
3.  **Review**: Displays detected columns.
4.  **Validation**: Shows a sample of the "DE -> PARA" mapping logic. User must approve.
5.  **Simulation**: Simulates the change on the first 2 rows of each table. User must approve.
6.  **Execution**: Applies the changes to the database in a transaction.
7.  **Audit**: Logs are written to `audit.log` and `rollback.csv`.

## Dependencies

- `sqlalchemy`: Database ORM and connection.
- `pyodbc`: ODBC driver support.
- `pandas` & `numpy`: Data manipulation.
- `scikit-learn`: Machine Learning (Logistic Regression).
- `Faker`: Semantic data generation.
- `python-dotenv`: Environment configuration.

## License

Proprietary / Internal Use.
