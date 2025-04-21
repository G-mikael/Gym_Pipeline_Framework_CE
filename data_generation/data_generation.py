import sqlite3
from faker import Faker
import random
import csv
from datetime import date
import os

# Criar banco SQLite
conn = sqlite3.connect("clientes.db")
cursor = conn.cursor()

# Criar tabelas
cursor.execute("""
CREATE TABLE IF NOT EXISTS Cliente (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    CPF TEXT UNIQUE,
    Nome TEXT,
    Endereco TEXT,
    DataNasc DATE
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS Transacao (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    ClienteID INTEGER,
    Data DATE,
    Valor REAL,
    Moeda TEXT,
    FOREIGN KEY (ClienteID) REFERENCES Cliente(ID)
)
""")

# Configurações
locales = ['pt_BR', 'en_US', 'fr_FR', 'es_ES', 'de_DE']
fakers = [Faker(locale) for locale in locales]
moedas = ['BRL', 'USD', 'EUR', 'JPY', 'GBP']

# Lista para clientes brasileiros
clientes_brasileiros = []

# Gerar clientes
n_clientes = 50000
for _ in range(n_clientes):
    locale_idx = random.randint(0, len(locales) - 1)
    fake = fakers[locale_idx]
    nome = fake.name()
    endereco = fake.address().replace("\n", ", ")
    data_nasc = fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat()
    cpf = fake.unique.ssn() if hasattr(fake, 'ssn') else fake.unique.ein()

    cursor.execute("""
        INSERT INTO Cliente (CPF, Nome, Endereco, DataNasc)
        VALUES (?, ?, ?, ?)
    """, (cpf, nome, endereco, data_nasc))

    if locales[locale_idx] == 'pt_BR':
        # gerar infos de score
        limite = round(random.uniform(500, 20000), 2)
        score = random.randint(300, 1000)
        renda = round(random.uniform(1200, 20000), 2)
        data_att = date.today().isoformat()
        clientes_brasileiros.append([cpf, limite, score, renda, data_att])

conn.commit()

# Gerar transações
cursor.execute("SELECT ID FROM Cliente")
clientes = cursor.fetchall()

faker = Faker()
for cliente in clientes:
    cliente_id = cliente[0]
    n_transacoes = random.randint(1, 20)
    for _ in range(n_transacoes):
        data = faker.date_between(start_date='-2y', end_date='today')
        valor = round(random.uniform(5, 5000), 2)
        moeda = random.choice(moedas)

        cursor.execute("""
            INSERT INTO Transacao (ClienteID, Data, Valor, Moeda)
            VALUES (?, ?, ?, ?)
        """, (cliente_id, data, valor, moeda))

conn.commit()
conn.close()

# Exportar CSV com os scores
with open("clientes_brasileiros_score.csv", mode="w", newline="", encoding="utf-8") as arquivo_csv:
    writer = csv.writer(arquivo_csv)
    writer.writerow(["CPF", "LimiteCredito", "Score", "RendaMensal", "DataAtualizacao"])
    writer.writerows(clientes_brasileiros)

print(f"Arquivo 'clientes_brasileiros_score.csv' criado com {len(clientes_brasileiros)} registros.")