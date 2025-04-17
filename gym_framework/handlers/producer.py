import time
from pathlib import Path
from gym_framework.sources.base_source import CSVSource, DBSource, TXTSource, DictSource
from gym_framework.handlers.base_handler import BaseHandler  # Supondo que esteja em outro módulo
from gym_framework.tests.mock.generate_data import generate_clients, generate_transactions, NUM_CLIENTS, NUM_NEW_TRANSACTIONS, NUM_TRANSACTIONS

BASE_DIR = Path(__file__).parent.resolve()

ID_TRANS = NUM_TRANSACTIONS + NUM_NEW_TRANSACTIONS
ID_CLIENT = NUM_CLIENTS


class ScoreCSVProducerHandler(BaseHandler):
    def handle(self, context, data=None):
        #time.sleep(1)
        print("[ScoreCSVProducerHandler] Extraindo dados de CSV...")

        csv_path = BASE_DIR / "mock_score.csv"
        csv_source = CSVSource(csv_path)
        df = csv_source.get_extractor().extract()

        # print("[ScoreCSVProducerHandler] Primeiras linhas:")
        # for row in df.data[:5]:
        #     print(row)

        self.send(context, df)


class ClientsDBProducerHandler(BaseHandler):
    def handle(self, context, data=None):
        #time.sleep(1)
        print("[ClientsDBProducerHandler] Extraindo dados de clients...")

        db_path = BASE_DIR / "mock_transactions.db"
        query = "SELECT * FROM clients"
        db_source = DBSource(db_path, query)
        df = db_source.get_extractor().extract()

        # print("[ClientsDBProducerHandler] Primeiras linhas:")
        # for row in df.data[:10]:
        #     print(row)

        self.send(context, df)


class TransactionsDBProducerHandler(BaseHandler):
    def handle(self, context, data=None):
        #time.sleep(1)
        print("[TransactionsDBProducerHandler] Extraindo dados de transactions...")

        db_path = BASE_DIR / "mock_transactions.db"
        query = "SELECT * FROM transactions"
        db_source = DBSource(db_path, query)
        df = db_source.get_extractor().extract()

        # print("[TransactionsDBProducerHandler] Primeiras linhas:")
        # print(df.showfirstrows(10))

        self.send(context, df)


class NewTransactionsTXTProducerHandler(BaseHandler):
    def handle(self, context, data=None):
        #time.sleep(1)
        print("[NewTransactionsTXTProducerHandler] Extraindo dados do .txt...")

        txt_path = BASE_DIR / "mock_new_transactions.txt"
        txt_source = TXTSource(txt_path)
        df = txt_source.get_extractor().extract()

        # print("[NewTransactionsTXTProducerHandler] Primeiras linhas:")
        # print(df.showfirstrows(10))

        self.send(context, df)
    


class TriggerTransactionsProducerHandler(BaseHandler):
    def __init__(self, num_transactions = 100):
        self.id_transaction = ID_TRANS
        self.new_transactions = num_transactions
        self.interval = 3


    def handle(self, context, data=None):

        while True:
            time.sleep(self.interval)
            trans = generate_transactions(self.new_transactions, self.id_transaction)

            dict_source = DictSource(trans)
            dict_extrator = dict_source.get_extractor()
            df_dict = dict_extrator.extract()

            self.id_transaction += self.new_transactions

            self.send(context, df_dict)



if __name__ == "__main__":
    
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