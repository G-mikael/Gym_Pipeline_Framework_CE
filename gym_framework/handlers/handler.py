from .base_handler import BaseHandler
import time
import unicodedata
import re
import multiprocessing
from gym_framework.core.dataframe import Dataframe
from .base_handler import BaseHandler
from gym_framework.sources.base_source import CSVSource, DBSource, TXTSource
from pathlib import Path

class ProdutoHandler(BaseHandler):
    def handle(self, data):
        time.sleep(1)
        print("Extraindo dados...")
        
        BASE_DIR = Path(__file__).parent.resolve()

        db_path = BASE_DIR / "mock_transactions.db"


        print("\n=== Testando mock_transactions - clients ===")
        query = "SELECT * FROM clients"
        db_source = DBSource(db_path, query)
        df_db = db_source.get_extractor()
        df_db = df_db.extract()
        print("Tipo: ", type(df_db))
        print(df_db.showfirstrows(5))

        print("1--------------------")

        return df_db

class NormalizerHandler(BaseHandler):
    def __init__(self, num_processes=None):
        self.num_processes = num_processes or multiprocessing.cpu_count()
        print(f"Using {self.num_processes} processes for normalization.")

    def normalize_text(self, text):
        text = unicodedata.normalize('NFD', text)
        text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
        text = text.upper()
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def simple_replace(self, text):
        substitutions = {
            "RUA": "R.",
            "AVENIDA": "AV.",
            "PRAÇA": "PÇA."
        }
        for original, replacement in substitutions.items():
            text = text.replace(original, replacement)
        return text

    def normalize_row(self, row):
        new_row = row.copy()
        for key, value in new_row.items():
            if isinstance(value, str):
                value = self.normalize_text(value)
                value = self.simple_replace(value)
                new_row[key] = value
        return new_row

    def handle(self, df: Dataframe) -> Dataframe:
        data = df[0].to_dict()

        normalized_data = [self.normalize_row(row) for row in data]

        return Dataframe(normalized_data, df[0].columns)

class LoaderHandler(BaseHandler):
    def handle(self, data):
        time.sleep(1)
        print("Carregando dados...")
        print("Resultado final:", data)

        print("3--------------------")

        return True