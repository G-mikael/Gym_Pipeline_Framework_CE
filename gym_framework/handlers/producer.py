import time
from pathlib import Path
from gym_framework.sources.base_source import CSVSource, DBSource, TXTSource, DictSource
from gym_framework.handlers.base_handler import BaseHandler
from gym_framework.core.dataframe import Dataframe
from gym_framework.tests.mock.generate_data import generate_clients, generate_transactions, NUM_CLIENTS, NUM_NEW_TRANSACTIONS, NUM_TRANSACTIONS
import uuid
import random

BASE_DIR = Path(__file__).parent.resolve()

ID_TRANS = NUM_TRANSACTIONS + NUM_NEW_TRANSACTIONS + 50000 
ID_CLIENT = NUM_CLIENTS


class ScoreCSVProducerHandler(BaseHandler):
    def handle(self, data=None):
        print(f"[ScoreCSVProducerHandler] Iniciando com data tipo: {type(data)}")
        if isinstance(data, list):
            if not data:
                print("[ScoreCSVProducerHandler] Batch RPC vazio recebido.")
                return Dataframe(columns=["cpf", "score", "renda_mensal", "limite_credito", "data_ultima_atualizacao", "batch_id", "event_id"])
            
            first_item_batch_id = data[0].get("batch_id", "unknown_batch")
            print(f"[ScoreCSVProducerHandler] Processando batch RPC com {len(data)} itens. Batch ID (primeiro item): {first_item_batch_id}")
            
            dict_source = DictSource(data)
            df = dict_source.get_extractor().extract()
            
            if "batch_id" not in df.columns:
                 print("[ScoreCSVProducerHandler] WARNING: batch_id não encontrado nas colunas do DataFrame de batch RPC.")
            if "event_id" not in df.columns:
                 print("[ScoreCSVProducerHandler] WARNING: event_id não encontrado nas colunas do DataFrame de batch RPC.")
            return df

        elif data is None or isinstance(data, (str, Path)):
            print("[ScoreCSVProducerHandler] Extraindo dados de CSV (modo arquivo)...")
            csv_path = Path(data) if data else BASE_DIR / "mock_score.csv"
            if not csv_path.exists():
                print(f"[ScoreCSVProducerHandler] Arquivo CSV não encontrado: {csv_path}")
                return Dataframe(columns=["cpf", "score", "renda_mensal", "limite_credito", "data_ultima_atualizacao", "batch_id", "event_id"])

            csv_source = CSVSource(csv_path)
            df = csv_source.get_extractor().extract()
            
            file_batch_id = f"file_batch_{str(uuid.uuid4())[:8]}"
            df.add_column("batch_id", [file_batch_id] * len(df.data))
            df.add_column("event_id", [f"file_event_{str(uuid.uuid4())[:8]}" for _ in range(len(df.data))])
            return df
        else:
            print(f"[ScoreCSVProducerHandler] Tipo de dado não suportado: {type(data)}")
            return Dataframe(columns=["cpf", "score", "renda_mensal", "limite_credito", "data_ultima_atualizacao", "batch_id", "event_id"])


class ClientsDBProducerHandler(BaseHandler):
    def handle(self, data=None):
        print(f"[ClientsDBProducerHandler] Iniciando com data tipo: {type(data)}")
        if isinstance(data, list):
            if not data:
                print("[ClientsDBProducerHandler] Batch RPC vazio recebido.")
                return Dataframe(columns=["id", "nome", "cpf", "data_nascimento", "endereco", "batch_id", "event_id"])

            first_item_batch_id = data[0].get("batch_id", "unknown_batch")
            print(f"[ClientsDBProducerHandler] Processando batch RPC com {len(data)} itens. Batch ID (primeiro item): {first_item_batch_id}")
            
            dict_source = DictSource(data)
            df = dict_source.get_extractor().extract()
            return df

        elif data is None or isinstance(data, (str, Path)):
            print("[ClientsDBProducerHandler] Extraindo dados de clients do DB (modo arquivo)...")
            db_path = Path(data) if data else BASE_DIR / "mock_transactions.db"
            if not db_path.exists():
                 print(f"[ClientsDBProducerHandler] Arquivo DB não encontrado: {db_path}")
                 return Dataframe(columns=["id", "nome", "cpf", "data_nascimento", "endereco", "batch_id", "event_id"])
            
            query = "SELECT * FROM clients" 
            db_source = DBSource(db_path, query)
            df = db_source.get_extractor().extract()

            file_batch_id = f"file_batch_{str(uuid.uuid4())[:8]}"
            df.add_column("batch_id", [file_batch_id] * len(df.data))
            df.add_column("event_id", [f"file_event_{str(uuid.uuid4())[:8]}" for _ in range(len(df.data))])
            return df
        else:
            print(f"[ClientsDBProducerHandler] Tipo de dado não suportado: {type(data)}")
            return Dataframe(columns=["id", "nome", "cpf", "data_nascimento", "endereco", "batch_id", "event_id"])


class TransactionsDBProducerHandler(BaseHandler):
    def handle(self, data=None):
        print(f"[TransactionsDBProducerHandler] Iniciando com data tipo: {type(data)}")
        if isinstance(data, list):
            if not data:
                print("[TransactionsDBProducerHandler] Batch RPC vazio recebido.")
                return Dataframe(columns=["id", "cliente_id", "data", "valor", "moeda", "categoria", "batch_id", "event_id"])
            
            first_item_batch_id = data[0].get("batch_id", "unknown_batch")
            print(f"[TransactionsDBProducerHandler] Processando batch RPC com {len(data)} itens. Batch ID (primeiro item): {first_item_batch_id}")
            
            dict_source = DictSource(data)
            df = dict_source.get_extractor().extract()
            return df
            
        elif data is None or isinstance(data, (str, Path)):
            print("[TransactionsDBProducerHandler] Extraindo dados de transactions do DB (modo arquivo)...")
            db_path = Path(data) if data else BASE_DIR / "mock_transactions.db"
            if not db_path.exists():
                 print(f"[TransactionsDBProducerHandler] Arquivo DB não encontrado: {db_path}")
                 return Dataframe(columns=["id", "cliente_id", "data", "valor", "moeda", "categoria", "batch_id", "event_id"])

            query = "SELECT * FROM transactions"
            db_source = DBSource(db_path, query)
            df = db_source.get_extractor().extract()

            file_batch_id = f"file_batch_{str(uuid.uuid4())[:8]}"
            df.add_column("batch_id", [file_batch_id] * len(df.data))
            df.add_column("event_id", [f"file_event_{str(uuid.uuid4())[:8]}" for _ in range(len(df.data))])
            return df
        else:
            print(f"[TransactionsDBProducerHandler] Tipo de dado não suportado: {type(data)}")
            return Dataframe(columns=["id", "cliente_id", "data", "valor", "moeda", "categoria", "batch_id", "event_id"])

class NewTransactionsTXTProducerHandler(BaseHandler):
    def handle(self, data=None):
        print(f"[NewTransactionsTXTProducerHandler] Iniciando com data tipo: {type(data)}")
        if isinstance(data, list):
            if not data:
                print("[NewTransactionsTXTProducerHandler] Batch RPC vazio recebido.")
                return Dataframe(columns=["id", "cliente_id", "data", "valor", "moeda", "categoria", "batch_id", "event_id"])

            first_item_batch_id = data[0].get("batch_id", "unknown_batch")
            print(f"[NewTransactionsTXTProducerHandler] Processando batch RPC com {len(data)} itens. Batch ID (primeiro item): {first_item_batch_id}")
            
            dict_source = DictSource(data)
            df = dict_source.get_extractor().extract()
            return df

        elif data is None or isinstance(data, (str, Path)):
            print("[NewTransactionsTXTProducerHandler] Extraindo dados de TXT (modo arquivo)...")
            txt_path = Path(data) if data else BASE_DIR / "mock_new_transactions.txt"
            if not txt_path.exists():
                print(f"[NewTransactionsTXTProducerHandler] Arquivo TXT não encontrado: {txt_path}")
                return Dataframe(columns=["id", "cliente_id", "data", "valor", "moeda", "categoria", "batch_id", "event_id"])
            
            txt_source = TXTSource(txt_path) 
            df = txt_source.get_extractor().extract()

            file_batch_id = f"file_batch_{str(uuid.uuid4())[:8]}"
            df.add_column("batch_id", [file_batch_id] * len(df.data))
            df.add_column("event_id", [f"file_event_{str(uuid.uuid4())[:8]}" for _ in range(len(df.data))])
            return df
        else:
            print(f"[NewTransactionsTXTProducerHandler] Tipo de dado não suportado: {type(data)}")
            return Dataframe(columns=["id", "cliente_id", "data", "valor", "moeda", "categoria", "batch_id", "event_id"])
    

class TriggerTransactionsProducerHandler(BaseHandler):
    def __init__(self, num_transactions=100, batch_id_prefix="triggered_batch"):
        self.id_transaction_start = ID_TRANS 
        self.new_transactions = num_transactions
        self.batch_id_prefix = batch_id_prefix

    def handle(self, data=None):
        current_batch_id = f"{self.batch_id_prefix}_{str(uuid.uuid4())[:8]}"
        local_id_transaction_start = random.randint(1000000, 2000000)

        print(f"[TriggerTransactionsProducerHandler] Gerando {self.new_transactions} novas transações. Batch ID: {current_batch_id}")

        generated_tx_list = generate_transactions(n=self.new_transactions, id_start=local_id_transaction_start)
        
        for tx in generated_tx_list:
            tx["batch_id"] = current_batch_id 
            tx["event_id"] = str(uuid.uuid4())
            
        dict_source = DictSource(generated_tx_list)
        df = dict_source.get_extractor().extract()
            
        return df

if __name__ == "__main__":
    print(BASE_DIR)
    
    cli = generate_clients(10, ID_CLIENT)
    trans = generate_transactions(10, ID_TRANS)

    print(cli)

    print("-"*60, "\n\n", "-"*60)

    print(trans)

    dict_source = DictSource(cli)
    dict_extrator = dict_source.get_extractor()
    df_dict = dict_extrator.extract()

    print(df_dict.showfirstrows(10))

    print("Tchau!")