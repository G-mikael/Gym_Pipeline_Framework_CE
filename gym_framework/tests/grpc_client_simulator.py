import grpc
import random
import datetime
from faker import Faker
import time

from gym_framework import event_ingestion_service_pb2
from gym_framework import event_ingestion_service_pb2_grpc

fake = Faker("pt_BR")

# --- Configurações ---
GRPC_SERVER_ADDRESS = 'localhost:50051'
MOEDAS = ["BRL", "USD", "EUR"]
TRANSACOES_CATEGORIAS = [
    "Alimentação", "Transporte", "Educação", "Saúde",
    "Lazer", "Moradia", "Compras", "Transferências", "Salário", "Outros"
]

# --- Funções de Geração de Dados (Adaptadas para gRPC messages) ---

def generate_client_data(client_id_val):
    """Gera dados para uma mensagem ClientData."""
    cpf = fake.cpf()
    client_msg = event_ingestion_service_pb2.ClientData(
        id=client_id_val,
        nome=fake.name(),
        cpf=cpf,
        data_nascimento=fake.date_of_birth(minimum_age=20, maximum_age=80).strftime("%Y-%m-%d"),
        endereco=f"{random.choice(['Rua', 'Avenida', 'Praça'])} {fake.street_name()}, {random.randint(1, 9999)}"
    )
    return client_msg, cpf 

def generate_transaction_data(transaction_id_val, client_id_val_for_tx, client_cpf_val_for_tx):
    """Gera dados para uma mensagem TransactionData."""
    dias_atras = random.randint(0, 365)
    transaction_msg = event_ingestion_service_pb2.TransactionData(
        id=transaction_id_val,
        cliente_id=client_id_val_for_tx,
        data=(datetime.datetime.now() - datetime.timedelta(days=dias_atras)).strftime("%Y-%m-%d"),
        valor=round(random.uniform(10, 5000), 2),
        moeda=random.choice(MOEDAS)
    )
    if random.choice([True, False]): # Categoria opcional
        transaction_msg.categoria = random.choice(TRANSACOES_CATEGORIAS)
    return transaction_msg

def generate_score_data(client_cpf_val):
    """Gera dados para uma mensagem ScoreData."""
    renda = round(random.uniform(1500, 20000), 2)
    score_msg = event_ingestion_service_pb2.ScoreData(
        cpf=client_cpf_val,
        score=random.randint(300, 900),
        renda_mensal=renda,
        limite_credito=round(renda * random.uniform(0.5, 2.0), 2),
        data_ultima_atualizacao=fake.date_between(start_date='-30d', end_date='today').strftime("%Y-%m-%d")
    )
    return score_msg

# --- Funções de Envio gRPC ---

def send_client_event(stub, client_data_msg):
    try:
        response = stub.IngestClient(client_data_msg)
        print(f"Cliente enviado: ID {client_data_msg.id}, CPF {client_data_msg.cpf}. Resposta: Success={response.success}, Msg='{response.message}', EventID='{response.event_id}'")
        return response.success
    except grpc.RpcError as e:
        print(f"Erro ao enviar cliente ID {client_data_msg.id}: {e}")
        return False

def send_transaction_event(stub, transaction_data_msg):
    try:
        response = stub.IngestTransaction(transaction_data_msg)
        print(f"Transação enviada: ID {transaction_data_msg.id}, Cliente ID {transaction_data_msg.cliente_id}. Resposta: Success={response.success}, Msg='{response.message}', EventID='{response.event_id}'")
        return response.success
    except grpc.RpcError as e:
        print(f"Erro ao enviar transação ID {transaction_data_msg.id}: {e}")
        return False

def send_score_event(stub, score_data_msg):
    try:
        response = stub.IngestScore(score_data_msg)
        print(f"Score enviado: Cliente CPF {score_data_msg.cpf}. Resposta: Success={response.success}, Msg='{response.message}', EventID='{response.event_id}'")
        return response.success
    except grpc.RpcError as e:
        print(f"Erro ao enviar score para CPF {score_data_msg.cpf}: {e}")
        return False

# --- Lógica Principal da Simulação ---

def run_simulation(stub, num_clients=5, num_transactions_per_client=2, num_scores_to_send=3):
    print(f"Iniciando simulação: {num_clients} clientes, {num_transactions_per_client} tx/cliente, {num_scores_to_send} scores.")

    generated_clients_info = []
    client_id_counter = 1
    transaction_id_counter = 1

    # 1. Gerar e enviar clientes
    print("\\n--- Enviando Clientes ---")
    for i in range(num_clients):
        client_msg, client_cpf = generate_client_data(client_id_counter)
        if send_client_event(stub, client_msg):
            generated_clients_info.append({"id": client_id_counter, "cpf": client_cpf})
        client_id_counter += 1
        time.sleep(random.uniform(0.05, 0.2)) # Pequeno delay

    if not generated_clients_info:
        print("Nenhum cliente foi enviado com sucesso. Encerrando simulação.")
        return

    # 2. Gerar e enviar transações para os clientes criados
    print("\\n--- Enviando Transações ---")
    for client_info in generated_clients_info:
        for _ in range(num_transactions_per_client):
            transaction_msg = generate_transaction_data(
                transaction_id_counter,
                client_info["id"],
                client_info["cpf"]
            )
            send_transaction_event(stub, transaction_msg)
            transaction_id_counter += 1
            time.sleep(random.uniform(0.05, 0.2)) # Pequeno delay

    # 3. Gerar e enviar scores (usando CPFs de clientes aleatórios já criados)
    print("\\n--- Enviando Scores ---")
    for _ in range(num_scores_to_send):
        if generated_clients_info:
            random_client_info = random.choice(generated_clients_info)
            score_msg = generate_score_data(random_client_info["cpf"])
            send_score_event(stub, score_msg)
            time.sleep(random.uniform(0.05, 0.2)) # Pequeno delay
        else:
            break # Sem clientes para associar scores

    print("\\nSimulação concluída.")

if __name__ == '__main__':
    print(f"Conectando ao servidor gRPC em {GRPC_SERVER_ADDRESS}...")
    try:
        channel = grpc.insecure_channel(GRPC_SERVER_ADDRESS)
        try:
            grpc.channel_ready_future(channel).result(timeout=5)
            print("Canal gRPC conectado com sucesso.")
        except grpc.FutureTimeoutError:
            print(f"Falha ao conectar ao servidor gRPC em {GRPC_SERVER_ADDRESS} após timeout. Verifique se o servidor está rodando.")
            exit(1)
            
        stub = event_ingestion_service_pb2_grpc.EventIngestionServiceStub(channel)

        num_sim_clients = 10
        num_sim_transactions_per_client = 5
        num_sim_scores = 7

        run_simulation(stub, num_sim_clients, num_sim_transactions_per_client, num_sim_scores)

    except Exception as e:
        print(f"Um erro ocorreu durante a execução do cliente gRPC: {e}")
    finally:
        if 'channel' in locals() and channel:
            channel.close()
            print("Canal gRPC fechado.")
