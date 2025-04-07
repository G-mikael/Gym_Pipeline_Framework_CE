import time
from pathlib import Path
from gym_framework.sources.base_source import CSVSource, DBSource, TXTSource
from gym_framework.handlers.base_handler import BaseHandler  # Supondo que esteja em outro módulo


BASE_DIR = Path(__file__).parent.resolve()


class ScoreCSVProducerHandler(BaseHandler):
    def handle(self, data=None):
        #time.sleep(1)
        print("[ScoreCSVProducerHandler] Extraindo dados de CSV...")

        csv_path = BASE_DIR / "mock_score.csv"
        csv_source = CSVSource(csv_path)
        df = csv_source.get_extractor().extract()

        # print("[ScoreCSVProducerHandler] Primeiras linhas:")
        # for row in df.data[:5]:
        #     print(row)

        return df


class ClientsDBProducerHandler(BaseHandler):
    def handle(self, data=None):
        #time.sleep(1)
        print("[ClientsDBProducerHandler] Extraindo dados de clients...")

        db_path = BASE_DIR / "mock_transactions.db"
        query = "SELECT * FROM clients"
        db_source = DBSource(db_path, query)
        df = db_source.get_extractor().extract()

        # print("[ClientsDBProducerHandler] Primeiras linhas:")
        # for row in df.data[:10]:
        #     print(row)

        return df


class TransactionsDBProducerHandler(BaseHandler):
    def handle(self, data=None):
        #time.sleep(1)
        print("[TransactionsDBProducerHandler] Extraindo dados de transactions...")

        db_path = BASE_DIR / "mock_transactions.db"
        query = "SELECT * FROM transactions"
        db_source = DBSource(db_path, query)
        df = db_source.get_extractor().extract()

        # print("[TransactionsDBProducerHandler] Primeiras linhas:")
        # print(df.showfirstrows(10))

        return df


class NewTransactionsTXTProducerHandler(BaseHandler):
    def handle(self, data=None):
        #time.sleep(1)
        print("[NewTransactionsTXTProducerHandler] Extraindo dados do .txt...")

        txt_path = BASE_DIR / "mock_new_transactions.txt"
        txt_source = TXTSource(txt_path)
        df = txt_source.get_extractor().extract()

        # print("[NewTransactionsTXTProducerHandler] Primeiras linhas:")
        # print(df.showfirstrows(10))

        return df
