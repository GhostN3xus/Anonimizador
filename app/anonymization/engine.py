import sqlite3
import os
import logging
from faker import Faker
from app.config import Config

class Anonymizer:
    def __init__(self):
        self.logger = logging.getLogger("Anonymizer")
        self.fake = Faker('pt_BR') # Portuguese context
        self.db_path = Config.ANONYMIZATION_DB_PATH
        self.conn = sqlite3.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        c = self.conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS mapping
                     (original_value TEXT, type TEXT, fake_value TEXT,
                      PRIMARY KEY (original_value, type))''')
        self.conn.commit()

    def get_fake_value(self, original_value, type_label):
        if original_value is None:
            return None

        original_str = str(original_value)
        if not original_str.strip():
            return original_value

        c = self.conn.cursor()
        c.execute("SELECT fake_value FROM mapping WHERE original_value = ? AND type = ?", (original_str, type_label))
        row = c.fetchone()

        if row:
            fake_val = row[0]
        else:
            fake_val = self._generate_fake(type_label, original_value)
            try:
                c.execute("INSERT INTO mapping (original_value, type, fake_value) VALUES (?, ?, ?)",
                          (original_str, type_label, fake_val))
                self.conn.commit()
            except sqlite3.IntegrityError:
                c.execute("SELECT fake_value FROM mapping WHERE original_value = ? AND type = ?", (original_str, type_label))
                row = c.fetchone()
                if row:
                    fake_val = row[0]

        return fake_val

    def _generate_fake(self, type_label, original_value=None):
        if type_label == 'NAME':
            return self.fake.name()
        elif type_label == 'EMAIL':
            return self.fake.email()
        elif type_label == 'CPF_CNPJ':
            # Heuristic detection
            s = str(original_value) if original_value else ""
            clean = ''.join(filter(str.isdigit, s))
            if len(clean) > 11 or '/' in s:
                return self.fake.cnpj()
            return self.fake.cpf()
        elif type_label == 'PHONE':
            return self.fake.phone_number()
        elif type_label == 'LOGIN':
            return self.fake.user_name()
        elif type_label == 'CREDIT_CARD':
            return self.fake.credit_card_number()
        elif type_label == 'TOKEN':
            return self.fake.sha256()[:20]
        elif type_label == 'NON_SENSITIVE':
            return str(self.fake.word())
        else:
            return self.fake.word()

    def get_mappings(self, limit=100):
        c = self.conn.cursor()
        c.execute("SELECT original_value, type, fake_value FROM mapping LIMIT ?", (limit,))
        rows = c.fetchall()
        return rows

    def close(self):
        if self.conn:
            self.conn.close()
