import time
from pathlib import Path
from gym_framework.sources.base_source import CSVSource, DBSource, TXTSource, DictSource
from gym_framework.handlers.base_handler import BaseHandler  # Supondo que esteja em outro módulo
from gym_framework.tests.mock.generate_data import generate_clients, generate_transactions, NUM_CLIENTS, NUM_NEW_TRANSACTIONS, NUM_TRANSACTIONS

BASE_DIR = Path(__file__).parent.resolve()

ID_TRANS = NUM_TRANSACTIONS + NUM_NEW_TRANSACTIONS
ID_CLIENT = NUM_CLIENTS


class ScoreCSVProducerHandler(BaseHandler):
    def handle(self, data=None):
        #time.sleep(1)
        print("[ScoreCSVProducerHandler] Extraindo dados de CSV...")

        if data is None:
            csv_path = BASE_DIR / "mock_score.csv"
        else:
            csv_path = Path(data)

        csv_source = CSVSource(csv_path)
        df = csv_source.get_extractor().extract()

        return df


class ClientsDBProducerHandler(BaseHandler):
    def handle(self, data=None):
        #time.sleep(1)
        print("[ClientsDBProducerHandler] Extraindo dados de clients...")

        if data is None:
            db_path = BASE_DIR / "mock_transactions.db"
        else:
            db_path = Path(data)

        query = "SELECT * FROM clients"
        db_source = DBSource(db_path, query)
        df = db_source.get_extractor().extract()

        return df


class TransactionsDBProducerHandler(BaseHandler):
    def handle(self, data=None):
        #time.sleep(1)
        print("[TransactionsDBProducerHandler] Extraindo dados de transactions...")

        if data is None:
            db_path = BASE_DIR / "mock_transactions.db"
        else:
            db_path = Path(data)

        
        query = "SELECT * FROM transactions"
        db_source = DBSource(db_path, query)
        df = db_source.get_extractor().extract()

        return df


class NewTransactionsTXTProducerHandler(BaseHandler):
    def handle(self, data=None):
        print("[NewTransactionsTXTProducerHandler] Extraindo dados do .txt...")

        if data is None:
            txt_path = BASE_DIR / "mock_new_transactions.txt"
        else:
            txt_path = Path(data)

        txt_source = TXTSource(txt_path)
        df = txt_source.get_extractor().extract()

        return df
    


class TriggerTransactionsProducerHandler(BaseHandler):
    def __init__(self, num_transactions=100):
        self.id_transaction = ID_TRANS 
        self.new_transactions = num_transactions

    def handle(self, data=None):
        print(f"[TriggerTransactionsProducerHandler] Gerando novas transações... {self.id_transaction}") # Resolver ID

        trans = generate_transactions(self.new_transactions, self.id_transaction)
        dict_source = DictSource(trans)
        dict_extractor = dict_source.get_extractor()
        df_dict = dict_extractor.extract()

        self.id_transaction += self.new_transactions
        return df_dict




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