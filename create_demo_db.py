import sqlite3
import os

db_path = "test.db"
if os.path.exists(db_path):
    print(f"Database {db_path} already exists.")
else:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create tables
    cursor.execute('''CREATE TABLE customers (
        id INTEGER PRIMARY KEY,
        full_name TEXT,
        email TEXT,
        cpf TEXT,
        phone TEXT,
        credit_card TEXT,
        signup_date TEXT
    )''')

    # Insert data
    data = [
        (1, 'Alice Smith', 'alice@example.com', '123.456.789-00', '(11) 91234-5678', '4444-5555-6666-7777', '2023-01-01'),
        (2, 'Bob Jones', 'bob.jones@gmail.com', '111.222.333-44', '11988887777', '1234-1234-1234-1234', '2023-02-15'),
        (3, 'Charlie Brown', 'charlie.b@corp.co', '999.888.777-66', '+5511999990000', '5555-4444-3333-2222', '2023-03-20')
    ]

    cursor.executemany('INSERT INTO customers VALUES (?,?,?,?,?,?,?)', data)

    conn.commit()
    conn.close()
    print(f"Created {db_path} with sample data.")
