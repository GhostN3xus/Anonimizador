import pandas as pd
import numpy as np
import re
import pickle
import os
import math
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from app.config import Config
import logging

class SensitiveDataClassifier:
    def __init__(self):
        self.model = None
        self.scaler = None
        self.logger = logging.getLogger("SensitiveDataClassifier")
        self.labels = ['NAME', 'EMAIL', 'CPF_CNPJ', 'PHONE', 'LOGIN', 'TOKEN', 'CREDIT_CARD', 'NON_SENSITIVE']
        self.model_path = getattr(Config, 'ML_MODEL_PATH', 'app/ml/trained_model.pkl')
        self.load_or_train()

    def load_or_train(self):
        if os.path.exists(self.model_path):
            try:
                with open(self.model_path, 'rb') as f:
                    saved_data = pickle.load(f)
                    self.model = saved_data['model']
                    self.scaler = saved_data['scaler']
                self.logger.info("Loaded existing ML model.")
            except Exception as e:
                self.logger.error(f"Failed to load model: {e}. Retraining.")
                self.train()
        else:
            self.logger.info("No model found. Training new model.")
            self.train()

    def train(self):
        X, y = self._generate_training_data()
        self.scaler = StandardScaler()
        X_scaled = self.scaler.fit_transform(X)

        self.model = LogisticRegression(max_iter=1000)
        self.model.fit(X_scaled, y)

        # Save
        with open(self.model_path, 'wb') as f:
            pickle.dump({'model': self.model, 'scaler': self.scaler}, f)
        self.logger.info("Model trained and saved.")

    def predict_column(self, samples, column_name, sql_type, stats, max_size=0):
        """
        Predicts the class of a column based on samples and metadata.
        Returns the class label and confidence score.
        """
        if not samples:
            return 'NON_SENSITIVE', 1.0

        # Extract features for each sample
        features_list = []
        for val in samples:
            f = self._extract_features(val, column_name, sql_type, stats, max_size)
            features_list.append(f)

        if not features_list:
            return 'NON_SENSITIVE', 1.0

        X = pd.DataFrame(features_list)
        X_scaled = self.scaler.transform(X)
        predictions = self.model.predict(X_scaled)

        # Majority vote
        from collections import Counter
        counts = Counter(predictions)
        most_common, count = counts.most_common(1)[0]
        confidence = count / len(predictions)

        return most_common, confidence

    def _extract_features(self, value, column_name, sql_type, stats, max_size=0):
        val_str = str(value) if value is not None else ""

        # 1. Value Features
        length = len(val_str)
        n_digits = sum(c.isdigit() for c in val_str)
        n_alpha = sum(c.isalpha() for c in val_str)
        n_special = length - n_digits - n_alpha

        pct_digits = n_digits / length if length > 0 else 0
        pct_alpha = n_alpha / length if length > 0 else 0
        pct_special = n_special / length if length > 0 else 0

        # Shannon Entropy
        entropy = 0
        if length > 0:
            prob = [float(val_str.count(c)) / length for c in dict.fromkeys(list(val_str))]
            entropy = - sum([p * math.log(p) / math.log(2.0) for p in prob])

        # Regex Flags (Boolean as 0/1)
        has_at = 1 if '@' in val_str else 0
        has_cpf_format = 1 if re.search(r'\d{3}\.\d{3}\.\d{3}-\d{2}', val_str) else 0
        has_cnpj_format = 1 if re.search(r'\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}', val_str) else 0
        has_card_format = 1 if re.search(r'\d{4}.?\d{4}.?\d{4}.?\d{4}', val_str) else 0

        # 2. Column Features
        col_lower = column_name.lower()
        name_has_email = 1 if 'email' in col_lower or 'mail' in col_lower else 0
        name_has_name = 1 if 'name' in col_lower or 'nome' in col_lower else 0
        name_has_cpf = 1 if 'cpf' in col_lower else 0
        name_has_cnpj = 1 if 'cnpj' in col_lower else 0
        name_has_phone = 1 if 'phone' in col_lower or 'cel' in col_lower or 'tel' in col_lower else 0
        name_has_login = 1 if 'login' in col_lower or 'user' in col_lower else 0
        name_has_pass = 1 if 'pass' in col_lower or 'senh' in col_lower or 'token' in col_lower else 0

        # 3. Contextual Features
        unique_ratio = stats.get('unique_ratio', 0)
        null_percentage = stats.get('null_percentage', 0)

        # 4. Mandatory SQL Features
        type_str = str(sql_type).lower()
        is_char = 1 if 'char' in type_str or 'text' in type_str or 'string' in type_str else 0
        is_int = 1 if 'int' in type_str else 0
        is_float = 1 if 'float' in type_str or 'real' in type_str or 'decimal' in type_str or 'numeric' in type_str or 'money' in type_str else 0

        size_feat = math.log(max_size + 1) if max_size > 0 else 0

        return [
            length, pct_digits, pct_alpha, pct_special, entropy,
            has_at, has_cpf_format, has_cnpj_format, has_card_format,
            name_has_email, name_has_name, name_has_cpf, name_has_cnpj, name_has_phone, name_has_login, name_has_pass,
            unique_ratio, null_percentage,
            is_char, is_int, is_float, size_feat
        ]

    def _generate_training_data(self):
        # Synthetic Data Generation
        data = []
        labels = []

        # Helpers
        def add_sample(val, col, label, stats={'unique_ratio': 0.9, 'null_percentage': 0.1}, sql_type='VARCHAR', max_size=255):
            data.append(self._extract_features(val, col, sql_type, stats, max_size))
            labels.append(label)

        # 1. EMAIL
        emails = ['john.doe@gmail.com', 'jane@corp.co', 'contact@site.org', 'a.b@c.com']
        for e in emails:
            add_sample(e, 'email', 'EMAIL')
            add_sample(e, 'user_email', 'EMAIL')
            add_sample(e, 'contato', 'EMAIL')

        # 2. CPF/CNPJ
        cpfs = ['123.456.789-00', '111.222.333-44', '98765432100', '12345678900']
        for c in cpfs:
            add_sample(c, 'cpf', 'CPF_CNPJ')
            add_sample(c, 'documento', 'CPF_CNPJ')

        # 3. NAME
        names = ['John Doe', 'Maria Silva', 'Jose Santos', 'Ana Souza']
        for n in names:
            add_sample(n, 'name', 'NAME')
            add_sample(n, 'full_name', 'NAME')
            add_sample(n, 'nome_completo', 'NAME')
            add_sample(n, 'cliente', 'NAME')

        # 4. PHONE
        phones = ['(11) 99999-9999', '11999999999', '+5511988887777', '3333-4444']
        for p in phones:
            add_sample(p, 'phone', 'PHONE')
            add_sample(p, 'telefone', 'PHONE')
            add_sample(p, 'celular', 'PHONE')

        # 5. CREDIT CARD
        cards = ['1234-5678-1234-5678', '4444555566667777', '1234 5678 1234 5678']
        for c in cards:
            add_sample(c, 'credit_card', 'CREDIT_CARD')
            add_sample(c, 'cartao', 'CREDIT_CARD')
            add_sample(c, 'cc_num', 'CREDIT_CARD')

        # 6. LOGIN/TOKEN
        logins = ['user123', 'admin', 'jsmith', 'root']
        for l in logins:
            add_sample(l, 'login', 'LOGIN')
            add_sample(l, 'username', 'LOGIN', max_size=50)

        tokens = ['akjsdhf78234', 'TOKEN_123', 'eyJhbGciOiJIUzI1NiIs']
        for t in tokens:
            add_sample(t, 'token', 'TOKEN', max_size=2000)
            add_sample(t, 'access_token', 'TOKEN')

        # 7. NON_SENSITIVE
        nons = ['ACTIVE', 'PENDING', '2023-01-01', '100.50', 'Category A', 'Product 1', 'Yes', 'No', '0', '1']
        for n in nons:
            add_sample(n, 'status', 'NON_SENSITIVE', {'unique_ratio': 0.05, 'null_percentage': 0}, max_size=20)
            add_sample(n, 'created_at', 'NON_SENSITIVE', sql_type='DATETIME')
            add_sample(n, 'amount', 'NON_SENSITIVE', sql_type='DECIMAL', max_size=10)
            add_sample(n, 'id', 'NON_SENSITIVE', sql_type='INTEGER', max_size=4)
            add_sample(n, 'description', 'NON_SENSITIVE', sql_type='TEXT', max_size=0) # TEXT usually max_size 0 or -1

        return data, labels
