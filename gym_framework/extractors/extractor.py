from gym_framework.core.dataframe import Dataframe
import os

class CSV_Extractor:
    def __init__(self, filepath):
        self.filepath = filepath

    def extract(self):
        """
        Fazer a leitura de um csv e retornar um dataframe a partir dele
        """
        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"Arquivo {self.filepath} não encontrado.")
        
        df = Dataframe.read_csv(self.filepath)
        return df