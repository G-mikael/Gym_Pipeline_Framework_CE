from abc import ABC, abstractmethod
from gym_framework.extractors.extractor import CSV_Extractor, DB_Extractor, TXT_Extractor, Dict_Extrator

class BaseSource(ABC):
    @abstractmethod
    def get_extractor(self):
        pass

class CSVSource(BaseSource):
    def __init__(self, filepath):
        self.filepath = filepath

    def get_extractor(self):
        return CSV_Extractor(self.filepath)

class DBSource(BaseSource):
    def __init__(self, db_path, query):
        self.db_path = db_path
        self.query = query

    def get_extractor(self):
        return DB_Extractor(self.db_path, self.query)
    
class TXTSource(BaseSource):
    def __init__(self, filepath, delimiter = ","):
        self.filepath = filepath
        self.delimiter = delimiter

    def get_extractor(self):
        return TXT_Extractor(self.filepath, delimiter=self.delimiter)
    
class DictSource(BaseSource):
    def __init__(self, dict):
        self.dict = dict

    def get_extractor(self):
        return Dict_Extrator(self.dict)