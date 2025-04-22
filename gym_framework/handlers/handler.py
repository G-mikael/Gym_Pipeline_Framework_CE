from gym_framework.handlers.base_handler import BaseHandler
import time
import unicodedata
import re
import multiprocessing
from gym_framework.core.dataframe import Dataframe
import random
import os
from datetime import datetime
import numpy as np
from datetime import datetime
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import classification_report, accuracy_score
from sklearn.model_selection import train_test_split
import time
import pickle
import sqlite3



class PipelineContext:
    def __init__(self, queue, dependencies=None, pipeline_queue=None):
        self.queue = queue
        self.dependencies = dependencies or []
        self.pipeline_queue = pipeline_queue


class NormalizerHandler(BaseHandler):
    def __init__(self, num_processes=None):
        self.num_processes = num_processes or multiprocessing.cpu_count()
        print(f"Using {self.num_processes} processes.")

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
        def add_random_transaction_column(df, column_name="categoria"):
            random_transactions = [random.choice(TRANSACOES) for _ in range(len(df.data))]
            df.add_column(column_name, random_transactions)

        # Exemplo de uso:
        add_random_transaction_column(data)

        return data
    
class SaveToCSVHandler(BaseHandler):
    # Pega data e hora atual
    data_hora_atual = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Cria o nome do arquivo com base na data e hora atual
    def handle(self, data, file_path = f"gym_framework/examples/results/transacoes_classificadas_{data_hora_atual}.csv"):
        """
        Salva o dataframe em um arquivo CSV.
        :param data: O dataframe que será salvo.
        :param file_path: O caminho do arquivo CSV onde o dataframe será salvo.
        """
        # Chama o método save_csv para salvar o dataframe no arquivo
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
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
        


class RiskTransactionClassifierHandler(BaseHandler):
    
    """
    Classe para classificar transações de risco usando um modelo de árvore de decisão.
    """

    def __init__(self, model_path="models/risk_model.pkl"):
        self.model_path = model_path
        self.modelo = None
        self.encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
        self._carregar_modelo()

    def _carregar_modelo(self):
        try:
            with open(self.model_path, "rb") as f:
                pacote = pickle.load(f)
                self.modelo = pacote["modelo"]
                self.encoder = pacote["encoder"]
            print("[RiskModelInferenceHandler] Modelo carregado com sucesso.")
        except Exception as e:
            print(f"[RiskModelInferenceHandler] Erro ao carregar modelo: {e}")

    def _preprocessar(self, linhas):
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
            except Exception as e:
                print(f"[RiskModelInferenceHandler] Erro em linha: {e}")
                continue

        moedas_cod = self.encoder.transform(moedas)
        return np.hstack([X_numerico, moedas_cod])

    def classificar(self, df):
        if self.modelo is None:
            print("[RiskModelInferenceHandler] Modelo não carregado.")
            return df

        X = self._preprocessar(df.data)
        predicoes = self.modelo.predict(X)
        df.add_column("suspeita", [int(p) for p in predicoes])
        return df

    def message(self):
        print("[RiskModelInferenceHandler] Classificando transações com modelo treinado...")

    def handle(self, df):
        #print(df.showfirstrows(10))
        return self.classificar(df)

class SaveToDatabaseHandler(BaseHandler):
    def __init__(self, db_path=f"gym_framework/examples/results/transacoes.db", table_name=None):
        self.db_path = db_path
        self.table_name = table_name or f"transacoes_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def handle(self, df):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Cria a tabela dinamicamente com base nas colunas
        columns_defs = []
        for col in df.columns:
            columns_defs.append(f'"{col}" TEXT')  # você pode melhorar o tipo dependendo dos dados
        columns_sql = ", ".join(columns_defs)
        cursor.execute(f'CREATE TABLE IF NOT EXISTS "{self.table_name}" ({columns_sql})')

        # Insere os dados
        for row in df.data:
            values = [str(row.get(col, "")) for col in df.columns]
            placeholders = ", ".join(["?"] * len(values))
            cursor.execute(f'INSERT INTO "{self.table_name}" VALUES ({placeholders})', values)

        conn.commit()
        conn.close()
        print(f"Tabela '{self.table_name}' salva no banco '{self.db_path}' com sucesso.")
        return True