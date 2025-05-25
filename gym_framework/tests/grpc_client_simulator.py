import grpc
import random
import datetime
from faker import Faker
import time
import uuid
import os
import sys
import pytz

# Adiciona o diretório raiz do projeto ao sys.path
# Isso garante que os módulos do projeto possam ser importados corretamente
# quando o script é executado como 'python -m gym_framework.tests.grpc_client_simulator'
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..')) # Sobe dois níveis: tests -> gym_framework -> project_root
if project_root not in sys.path:
    sys.path.insert(0, project_root)

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

# Constantes (podem ser ajustadas para testes de carga)
CLIENT_BATCH_SIZE = 5
TRANSACTION_BATCH_SIZE = 10 # 2 por cliente, se CLIENT_BATCH_SIZE = 5
SCORE_BATCH_SIZE = 3

NUM_CLIENT_BATCHES = 2
NUM_TRANSACTION_BATCHES_PER_CLIENT_BATCH = 1 # Cada lote de clientes gera X lotes de transações
NUM_SCORE_BATCHES = 2

DEFAULT_SERVER_ADDRESS = 'localhost:50051'

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

# --- Funções de Envio de Eventos em Batch ---
def send_client_batch(stub, client_messages):
    if not client_messages:
        return
    batch_id = f"client_batch_{str(uuid.uuid4())[:8]}"
    emission_ts_iso = datetime.datetime.now(pytz.utc).isoformat()
    request = event_ingestion_service_pb2.IngestClientsRequest(
        items=client_messages, 
        batch_id=batch_id,
        emission_timestamp_iso=emission_ts_iso
    )
    try:
        print(f"Enviando lote de {len(client_messages)} clientes (Batch ID: {batch_id}, Emissão: {emission_ts_iso})...")
        start_time = time.monotonic()
        response = stub.IngestClients(request, timeout=60) # Timeout aumentado para lotes e depuração
        end_time = time.monotonic()
        round_trip_duration = end_time - start_time
        print(f"Resposta do servidor (Clientes - Batch {response.batch_id}): Success={response.success}, Msg='{response.message}', "
              f"Recebidos={response.items_received}, Processados={response.items_processed_successfully}, "
              f"Duração Servidor={response.processing_duration_seconds:.4f}s, Round-trip={round_trip_duration:.4f}s")
    except grpc.RpcError as e:
        print(f"Erro RPC ao enviar lote de clientes (Batch ID: {batch_id}): {e.code()} - {e.details()}")

def send_transaction_batch(stub, transaction_messages):
    if not transaction_messages:
        return
    batch_id = f"tx_batch_{str(uuid.uuid4())[:8]}"
    emission_ts_iso = datetime.datetime.now(pytz.utc).isoformat()
    request = event_ingestion_service_pb2.IngestTransactionsRequest(
        items=transaction_messages, 
        batch_id=batch_id,
        emission_timestamp_iso=emission_ts_iso
    )
    try:
        print(f"Enviando lote de {len(transaction_messages)} transações (Batch ID: {batch_id}, Emissão: {emission_ts_iso})...")
        start_time = time.monotonic()
        response = stub.IngestTransactions(request, timeout=60)
        end_time = time.monotonic()
        round_trip_duration = end_time - start_time
        print(f"Resposta do servidor (Transações - Batch {response.batch_id}): Success={response.success}, Msg='{response.message}', "
              f"Recebidos={response.items_received}, Processados={response.items_processed_successfully}, "
              f"Duração Servidor={response.processing_duration_seconds:.4f}s, Round-trip={round_trip_duration:.4f}s")
    except grpc.RpcError as e:
        print(f"Erro RPC ao enviar lote de transações (Batch ID: {batch_id}): {e.code()} - {e.details()}")

def send_score_batch(stub, score_messages):
    if not score_messages:
        return
    batch_id = f"score_batch_{str(uuid.uuid4())[:8]}"
    emission_ts_iso = datetime.datetime.now(pytz.utc).isoformat()
    request = event_ingestion_service_pb2.IngestScoresRequest(
        items=score_messages, 
        batch_id=batch_id,
        emission_timestamp_iso=emission_ts_iso
    )
    try:
        print(f"Enviando lote de {len(score_messages)} scores (Batch ID: {batch_id}, Emissão: {emission_ts_iso})...")
        start_time = time.monotonic()
        response = stub.IngestScores(request, timeout=60)
        end_time = time.monotonic()
        round_trip_duration = end_time - start_time
        print(f"Resposta do servidor (Scores - Batch {response.batch_id}): Success={response.success}, Msg='{response.message}', "
              f"Recebidos={response.items_received}, Processados={response.items_processed_successfully}, "
              f"Duração Servidor={response.processing_duration_seconds:.4f}s, Round-trip={round_trip_duration:.4f}s")
    except grpc.RpcError as e:
        print(f"Erro RPC ao enviar lote de scores (Batch ID: {batch_id}): {e.code()} - {e.details()}")

# --- Lógica Principal da Simulação ---

def run_simulation(stub, num_client_batches, client_batch_size, 
                   num_tx_batches_per_client_batch, tx_batch_size, 
                   num_score_batches, score_batch_size):
    
    print(f"Iniciando simulação de batches:")
    print(f"  Clientes: {num_client_batches} batches de {client_batch_size}")
    print(f"  Transações: {num_tx_batches_per_client_batch} batches de {tx_batch_size} por batch de cliente")
    print(f"  Scores: {num_score_batches} batches de {score_batch_size}")

    client_id_counter = 1
    transaction_id_counter = 1
    
    generated_client_cpfs = [] 
    generated_client_ids = []

    # 1. Gerar e enviar lotes de clientes
    print("\n--- Enviando Lotes de Clientes ---")
    for batch_num in range(num_client_batches):
        client_messages_batch = []
        for _ in range(client_batch_size):
            client_msg, client_cpf = generate_client_data(client_id_counter)
            client_messages_batch.append(client_msg)
            generated_client_cpfs.append(client_cpf) 
            generated_client_ids.append(client_msg.id)  
            client_id_counter += 1
        send_client_batch(stub, client_messages_batch)
        time.sleep(random.uniform(0.1, 0.5))

    if not generated_client_ids:
        print("Nenhum cliente foi gerado/enviado. Encerrando simulação de transações e scores.")
        return

    print("\n--- Enviando Lotes de Transações ---")

    total_transaction_batches = num_client_batches * num_tx_batches_per_client_batch

    for batch_num in range(total_transaction_batches):
        transaction_messages_batch = []
        for _ in range(tx_batch_size):
            if not generated_client_ids: break
            random_client_id = random.choice(generated_client_ids)
            tx_msg = generate_transaction_data(transaction_id_counter, random_client_id, generated_client_cpfs[random_client_id - 1])
            transaction_messages_batch.append(tx_msg)
            transaction_id_counter += 1
        if transaction_messages_batch:
            send_transaction_batch(stub, transaction_messages_batch)
            time.sleep(random.uniform(0.1, 0.5))
        else:
            break 

    print("\n--- Enviando Lotes de Scores ---")
    if not generated_client_cpfs:
        print("Nenhum CPF de cliente disponível. Encerrando simulação de scores.")
    else:
        for batch_num in range(num_score_batches):
            score_messages_batch = []
            for _ in range(score_batch_size):
                if not generated_client_cpfs: break
                random_client_cpf = random.choice(generated_client_cpfs)
                score_msg = generate_score_data(random_client_cpf)
                score_messages_batch.append(score_msg)
            if score_messages_batch:
                send_score_batch(stub, score_messages_batch)
                time.sleep(random.uniform(0.1, 0.5))
            else:
                break
    
    print("\nSimulação de envio de batches concluída.")

if __name__ == '__main__':
    server_address = os.getenv("GRPC_SERVER_ADDRESS", DEFAULT_SERVER_ADDRESS)
    print(f"Conectando ao servidor gRPC em {server_address}...")
    try:
        if __package__ is None or __package__ == '':
            pass 
        from gym_framework import event_ingestion_service_pb2_grpc # type: ignore

    except ImportError as e:
        print(f"Erro de importação ao tentar carregar stubs gRPC: {e}")
        print("Certifique-se que os stubs gRPC foram gerados e estão no PYTHONPATH.")
        print(f"sys.path: {sys.path}")
        print(f"__name__: {__name__}, __package__: {__package__}")
        exit(1)

    channel = grpc.insecure_channel(server_address)
    stub = event_ingestion_service_pb2_grpc.EventIngestionServiceStub(channel)
    print("Conexão estabelecida.")

    try:
        run_simulation(stub, 
                       NUM_CLIENT_BATCHES, CLIENT_BATCH_SIZE, 
                       NUM_TRANSACTION_BATCHES_PER_CLIENT_BATCH, TRANSACTION_BATCH_SIZE,
                       NUM_SCORE_BATCHES, SCORE_BATCH_SIZE)
    except KeyboardInterrupt:
        print("\nSimulação interrompida pelo usuário.")
    finally:
        channel.close()
        print("Conexão fechada.")
