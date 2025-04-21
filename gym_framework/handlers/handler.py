from .base_handler import BaseHandler
import time
import unicodedata
import re
import multiprocessing
from gym_framework.core.dataframe import Dataframe
import random
import csv
from datetime import datetime
import numpy as np
from datetime import datetime
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import classification_report, accuracy_score
from sklearn.model_selection import train_test_split
import time


class PipelineContext:
    def __init__(self, queue, dependencies=None, pipeline_queue=None):
        self.queue = queue
        self.dependencies = dependencies or []
        self.pipeline_queue = pipeline_queue


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

    def handle(self, df: Dataframe):
        data = df.to_dict()

        normalized_data = [self.normalize_row(row) for row in data]

        return Dataframe(normalized_data, df.columns)

class LoaderHandler(BaseHandler):
    def handle(self, data):
        time.sleep(1)
        print("Carregando dados...")
        print("Resultado final:", data)
        print(data.showfirstrows(10))
        
        return True
    
class ClassifierHandler(BaseHandler):
    def handle(self, data):
        TRANSACOES = [
        "Alimentação", "Transporte", "Educação", "Saúde", "Lazer",
        "Moradia", "Compras", "Transferências", "Salário", "Outros"]

        # Função para adicionar uma coluna com transações aleatórias
        def add_random_transaction_column(df, column_name="Transacao"):
            random_transactions = [random.choice(TRANSACOES) for _ in range(len(df.data))]
            df.add_column(column_name, random_transactions)

        # Exemplo de uso:
        add_random_transaction_column(data)

        return data
    
class SaveToFileHandler(BaseHandler):
    def handle(self, data, file_path = "dataframe.csv"):
        """
        Salva o dataframe em um arquivo CSV.
        :param data: O dataframe que será salvo.
        :param file_path: O caminho do arquivo CSV onde o dataframe será salvo.
        """
        # Chama o método save_csv para salvar o dataframe no arquivo
        data.save_csv(file_path)
        print(f"Dataframe salvo com sucesso em {file_path}")

class CalculateAverageGainHandler(BaseHandler):
    def handle(self, data):
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
            return ganho_medio
        else:
            print(f"O cliente com id {random_id} não possui transações registradas.")
            return
        

class RiskTransactionClassifierHandler:
    def __init__(self):
        self.modelo = DecisionTreeClassifier()
        self.encoder = OneHotEncoder(sparse=False, handle_unknown='ignore')

    def _preprocessar(self, linhas, treinar_encoder=False):
        X_numerico = []
        moedas = []

        for row in linhas:
            try:
                valor = float(row['valor'])
                moeda = row['moeda']
                data = datetime.strptime(row['data'], '%Y-%m-%d')
                dia = data.weekday()
                mes = data.month
                X_numerico.append([valor, dia, mes])
                moedas.append([moeda])
            except:
                continue  # ignora linhas mal formatadas

        if treinar_encoder:
            moedas_cod = self.encoder.fit_transform(moedas)
        else:
            moedas_cod = self.encoder.transform(moedas)

        return np.hstack([X_numerico, moedas_cod])

    def treinar(self, df_rotulado, test_size=0.2):
        """
        Treina o modelo com um Dataframe contendo a coluna 'suspeita'.
        Separa automaticamente parte dos dados para teste.
        """
        linhas = df_rotulado.data
        X = self._preprocessar(linhas, treinar_encoder=True)
        y = np.array([int(row['suspeita']) for row in linhas])

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)

        self.modelo.fit(X_train, y_train)

        # Avaliação no teste
        y_pred = self.modelo.predict(X_test)
        print("===== AVALIAÇÃO NO CONJUNTO DE TESTE =====")
        print("Acurácia:", accuracy_score(y_test, y_pred))
        print("Relatório:\n", classification_report(y_test, y_pred))

    def classificar_novas(self, df_novas):
        """
        Recebe um Dataframe de novas transações e adiciona a coluna 'suspeita'.
        """
        linhas = df_novas.data
        X_novo = self._preprocessar(linhas)
        predicoes = self.modelo.predict(X_novo)

        df_novas.add_column("suspeita", [int(p) for p in predicoes])
        return df_novas  