from abc import ABC, abstractmethod
import os
import csv
import sqlite3
from gym_framework.core.dataframe import Dataframe

class Extractor(ABC):
    @abstractmethod
    def extract(self):
        pass

class CSV_Extractor(Extractor):
    def __init__(self, filepath):
        self.filepath = filepath

    def extract(self):
        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"Arquivo {self.filepath} não encontrado.")

        with open(self.filepath, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = [row for row in reader]
            columns = reader.fieldnames

        return Dataframe(data, columns)

class DB_Extractor(Extractor):
    def __init__(self, db_path, query):
        self.db_path = db_path
        self.query = query

    def extract(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(self.query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        conn.close()
        data = [dict(zip(columns, row)) for row in rows]
        return Dataframe(data, columns)
    
class TXT_Extractor(Extractor):
    def __init__(self, filepath, delimiter=","):
        self.filepath = filepath
        self.delimiter = delimiter

    def extract(self):
        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"Arquivo {self.filepath} não encontrado.")

        with open(self.filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            data = [row for row in reader]
            columns = reader.fieldnames

        return Dataframe(data, columns)
    
class Dict_Extrator(Extractor):
    def __init__(self, dict):
        self.dict = dict

    def extract(self):
        return Dataframe(self.dict)