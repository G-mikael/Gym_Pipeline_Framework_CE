from .base_handler import BaseHandler
import time
import unicodedata
import re
import multiprocessing
from gym_framework.core.dataframe import Dataframe
from .base_handler import BaseHandler
import random

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

    def handle(self, context, df: Dataframe):
        data = df.to_dict()

        normalized_data = [self.normalize_row(row) for row in data]

        self.send(context, Dataframe(normalized_data, df.columns))

class LoaderHandler(BaseHandler):
    def handle(self, context, data):
        time.sleep(1)
        print("Carregando dados...")
        print("Resultado final:", data)
        print(data.showfirstrows(10))
        
        return True
    
class ClassifierHandler(BaseHandler):
    def handle(self, context, data):
        TRANSACOES = [
        "Alimentação", "Transporte", "Educação", "Saúde", "Lazer",
        "Moradia", "Compras", "Transferências", "Salário", "Outros"]

        # Função para adicionar uma coluna com transações aleatórias
        def add_random_transaction_column(df, column_name="Transacao"):
            random_transactions = [random.choice(TRANSACOES) for _ in range(len(df.data))]
            df.add_column(column_name, random_transactions)

        # Exemplo de uso:
        add_random_transaction_column(data)

        self.send(context, data)
    
class SaveToFileHandler(BaseHandler):
    def handle(self, context, data, file_path = "dataframe.csv"):
        """
        Salva o dataframe em um arquivo CSV.
        :param data: O dataframe que será salvo.
        :param file_path: O caminho do arquivo CSV onde o dataframe será salvo.
        """
        # Chama o método save_csv para salvar o dataframe no arquivo
        data.save_csv(file_path)
        print(f"Dataframe salvo com sucesso em {file_path}")

class CalculateAverageGainHandler(BaseHandler):
    def handle(self, context, data):
        """
        Escolhe um cliente aleatório e calcula o ganho médio das suas transações.
        :param data: O dataframe contendo as transações.
        """
        # Seleciona um id aleatório do dataframe

        random_id = random.choice(data.get_column("id"))
        
        # Filtra as transações que pertencem ao cliente com o id selecionado
        cliente_transacoes = data.filter_rows(lambda row: row["id"] == random_id)
        
        # Filtra apenas os valores das transações para calcular a média
        valores = cliente_transacoes.get_column("valor")
        valores = [float(valor) for valor in valores]
        
        if valores:
            # Calcula o ganho médio
            ganho_medio = sum(valores) / len(valores)
            print(f"O cliente com id {random_id} tem um ganho médio de {ganho_medio:.2f}")
            self.send(context, ganho_medio)
        else:
            print(f"O cliente com id {random_id} não possui transações registradas.")
