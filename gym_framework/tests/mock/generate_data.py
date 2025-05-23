import sqlite3
import random
import datetime
from faker import Faker
import os
import json
import time

fake = Faker("pt_BR")  

NUM_TRANSACTIONS = 500000
NUM_NEW_TRANSACTIONS = 20000
NUM_CLIENTS = 100000

MOEDAS = ["BRL", "USD", "EUR"]
TRANSACOES = [
    "Alimentação",
    "Transporte",
    "Educação",
    "Saúde",
    "Lazer",
    "Moradia",
    "Compras",
    "Transferências",
    "Salário",
    "Outros"
]


# Geração de clientes simulados
def generate_clients(n=100, id_start=0):
    clients = []
    used_cpfs = set()
    i = 0
    
    while i < n:

        cpf = fake.cpf()
        if cpf in used_cpfs:
            continue
        used_cpfs.add(cpf)

        birth_date = fake.date_of_birth(minimum_age=20, maximum_age=80).strftime("%Y-%m-%d")    # Gera data de aniversário
        
        street_prefix = random.choice(["Rua", "Avenida", "Praça"])                              # Gera endereço não padronizado
        
        space_before = " " * random.randint(0, 2)       # Adiciona alguns espaços extras aleatórios
        space_after = " " * random.randint(0, 2)

        # Gera o endereço com variações
        address = f"{space_before}{street_prefix} {fake.street_name()}, {random.randint(1, 9999)}{space_after}"     

        client = {
            "id": i + id_start + 1,
            "nome": fake.name(),
            "cpf": cpf,
            "data_nascimento": birth_date,
            "endereco": address
        }
        clients.append(client)
        i += 1

    return clients


# Geração de transações
def generate_transactions(n=1000, client_count=100, new=False, id_start = 0):
    transactions = []
    for i in range(n):
        if new:
            dias_atras = random.randint(0, 30)  # Até 30 dias atrás
        else:
            dias_atras = random.randint(31, 365)  # Entre 31 dias e 1 ano atrás

        transaction = {
            "id": id_start + i + 1,
            "cliente_id": random.randint(0, client_count - 1),
            "data": (datetime.datetime.now() - datetime.timedelta(days=dias_atras)).strftime("%Y-%m-%d"),
            "valor": round(random.uniform(10, 5000), 2),
            "moeda": random.choice(MOEDAS),
        }

        if not new:
            transaction["categoria"] = random.choice(TRANSACOES)
            
        transactions.append(transaction)
    return transactions


# Salvar clientes em um banco SQLite
def save_clients_to_sqlite(clients, db_name="mock_transactions.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS clients (
                        id INTEGER PRIMARY KEY,
                        nome TEXT,
                        cpf TEXT UNIQUE,
                        data_nascimento TEXT,
                        endereco TEXT)''')
    
    for client in clients:
        cursor.execute("INSERT INTO clients (nome, cpf, data_nascimento, endereco) VALUES (?, ?, ?, ?)", 
                       (client["nome"], client["cpf"], client["data_nascimento"], client["endereco"]))
    conn.commit()
    conn.close()
    print(f"Clientes salvos no banco de dados SQLite {db_name}")


# Salvar transações em um banco SQLite
def save_transactions_to_sqlite(transactions, db_name="mock_transactions.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS transactions (
                        id INTEGER PRIMARY KEY,
                        cliente_id INTEGER,
                        data TEXT,
                        valor REAL,
                        moeda TEXT,
                        categoria TEXT,
                        FOREIGN KEY (cliente_id) REFERENCES clients(id))''')
    
    for transaction in transactions:
        cursor.execute("INSERT INTO transactions (cliente_id, data, valor, moeda, categoria) VALUES (?, ?, ?, ?, ?)", 
               (transaction["cliente_id"], transaction["data"], transaction["valor"], transaction["moeda"], transaction.get("categoria")))
    conn.commit()
    conn.close()
    print(f"Banco de dados SQLite salvo como {db_name}")


def save_transactions_to_txt(transactions, filename="mock_new_transactions.txt", print_message = True):
    with open(filename, "w", encoding="utf-8") as f:
        f.write("id,cliente_id,data,valor,moeda\n")
        for t in transactions:
            linha = f"{t['id']},{t['cliente_id']},{t['data']},{t['valor']},{t['moeda']}\n"
            f.write(linha)
    if print_message: print(f"Arquivo TXT de transações gerado: {filename}")

import csv

def save_scores_to_csv(clients, filename="mock_score.csv"):
    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["cpf", "score", "renda_mensal", "limite_credito", "data_ultima_atualizacao"])

        for client in clients:
            cpf = client["cpf"]
            score = random.randint(300, 900)
            renda = round(random.uniform(1500, 20000), 2)
            limite = round(renda * random.uniform(0.5, 2.0), 2)
            data_atualizacao = fake.date_between(start_date='-30d', end_date='today').strftime("%Y-%m-%d")
            writer.writerow([cpf, score, renda, limite, data_atualizacao])
    
    print(f"Arquivo CSV com score dos clientes gerado: {filename}")

def gerar_arquivos_txt_simulados(output_dir="data/incoming", max_files=5, transactions_per_file=100, client_count=100,delay_range=(4, 10)):
    os.makedirs(output_dir, exist_ok=True)

    created_files = []

    id_start = NUM_TRANSACTIONS + NUM_NEW_TRANSACTIONS

    for i in range(max_files):
        filename = f"trans_{int(time.time())}_{i}.txt"
        filepath = os.path.join(output_dir, filename)

        transactions = generate_transactions(
            n=transactions_per_file,
            client_count=client_count,
            new=True,
            id_start= id_start + (i * transactions_per_file)
        )

        # Usa sua função para salvar como CSV
        save_transactions_to_txt(transactions, filepath, False)
        created_files.append(filepath)

        pid = os.getpid()
        print(f"[Simulador externo | PID {pid}] Arquivo criado: {filename}")
        time.sleep(random.randint(*delay_range))

    # Apagar arquivos após tempo (opcional)
    # time.sleep(30)
    # for file in created_files:
    #     try:
    #         os.remove(file)
    #         print(f"[Simulador externo] Arquivo apagado: {file}")
    #     except Exception as e:
    #         print(f"[Erro ao apagar {file}]: {e}")


if __name__ == "__main__":
    clients = generate_clients(NUM_CLIENTS)
    transactions = generate_transactions(NUM_TRANSACTIONS, client_count=len(clients))
    novas_transacoes = generate_transactions(NUM_NEW_TRANSACTIONS, len(clients), True, NUM_TRANSACTIONS)
    
    save_clients_to_sqlite(clients)
    save_transactions_to_sqlite(transactions)
    save_transactions_to_txt(novas_transacoes)
    save_scores_to_csv(clients)