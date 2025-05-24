import grpc
import random
import datetime
from faker import Faker
import time

from gym_framework import event_ingestion_service_pb2
from gym_framework import event_ingestion_service_pb2_grpc
from gym_framework.rpc_server import serve

fake = Faker("pt_BR")

# --- Configurações ---
GRPC_SERVER_ADDRESS = 'localhost:50051'
MOEDAS = ["BRL", "USD", "EUR"]
TRANSACOES_CATEGORIAS = [
    "Alimentação", "Transporte", "Educação", "Saúde",
    "Lazer", "Moradia", "Compras", "Transferências", "Salário", "Outros"
]

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataGeneratorClient:
    def __init__(self):
        self.channel = grpc.insecure_channel(GRPC_SERVER_ADDRESS)
        self.stub = event_ingestion_service_pb2_grpc.EventIngestionServiceStub(self.channel)
        self.client_counter = 1
        self.transaction_counter = 1

    def generate_client_data(self):
        """Gera dados para uma mensagem ClientData."""
        cpf = fake.cpf()
        client_msg = event_ingestion_service_pb2.ClientData(
            id=self.client_counter,
            nome=fake.name(),
            cpf=cpf,
            data_nascimento=fake.date_of_birth(minimum_age=20, maximum_age=80).strftime("%Y-%m-%d"),
            endereco=f"{random.choice(['Rua', 'Avenida', 'Praça'])} {fake.street_name()}, {random.randint(1, 9999)}"
        )
        self.client_counter += 1
        return client_msg, cpf

    def generate_transaction_data(self, client_id, client_cpf):
        """Gera dados para uma mensagem TransactionData."""
        dias_atras = random.randint(0, 365)
        transaction_msg = event_ingestion_service_pb2.TransactionData(
            id=self.transaction_counter,
            cliente_id=client_id,
            data=(datetime.datetime.now() - datetime.timedelta(days=dias_atras)).strftime("%Y-%m-%d"),
            valor=round(random.uniform(10, 5000), 2),
            moeda=random.choice(MOEDAS)
        )
        if random.choice([True, False]): # Categoria opcional
            transaction_msg.categoria = random.choice(TRANSACOES_CATEGORIAS)
        self.transaction_counter += 1
        return transaction_msg

    def generate_score_data(self, client_cpf):
        """Gera dados para uma mensagem ScoreData."""
        renda = round(random.uniform(1500, 20000), 2)
        return event_ingestion_service_pb2.ScoreData(
            cpf=client_cpf,
            score=random.randint(300, 900),
            renda_mensal=renda,
            limite_credito=round(renda * random.uniform(0.5, 2.0), 2),
            data_ultima_atualizacao=fake.date_between(start_date='-30d', end_date='today').strftime("%Y-%m-%d")
        )

    def run_simulation(self, num_clients=5, num_transactions_per_client=2, num_scores=3):
        print(f"Iniciando simulação: {num_clients} clientes, {num_transactions_per_client} tx/cliente, {num_scores} scores.")

        clients = []
        
        # Gerar e enviar clientes
        for _ in range(num_clients):
            client_msg, client_cpf = self.generate_client_data()
            try:
                response = self.stub.IngestClient(client_msg)
                if response.success:
                    clients.append((client_msg.id, client_cpf))
                    print(f"Cliente {client_msg.id} enviado com sucesso")
            except grpc.RpcError as e:
                print(f"Erro ao enviar cliente: {e}")
            time.sleep(0.1)

        # Gerar transações para cada cliente
        for client_id, client_cpf in clients:
            for _ in range(num_transactions_per_client):
                tx_msg = self.generate_transaction_data(client_id, client_cpf)
                try:
                    response = self.stub.IngestTransaction(tx_msg)
                    if response.success:
                        print(f"Transação {tx_msg.id} para cliente {client_id} enviada")
                except grpc.RpcError as e:
                    print(f"Erro ao enviar transação: {e}")
                time.sleep(0.1)

        # Gerar scores para clientes aleatórios
        for _ in range(num_scores):
            if clients:
                _, random_cpf = random.choice(clients)
                score_msg = self.generate_score_data(random_cpf)
                try:
                    response = self.stub.IngestScore(score_msg)
                    if response.success:
                        print(f"Score para CPF {random_cpf} enviado")
                except grpc.RpcError as e:
                    print(f"Erro ao enviar score: {e}")
                time.sleep(0.1)

        print("Simulação concluída")

if __name__ == '__main__':
    try:
        client = DataGeneratorClient()
        client.run_simulation(
            num_clients=10,
            num_transactions_per_client=5,
            num_scores=7
        )
    except Exception as e:
        print(f"Erro na simulação: {e}")
    finally:
        if hasattr(client, 'channel'):
            client.channel.close()