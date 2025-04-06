import csv
import sqlite3
import os

# Caminho da pasta atual do script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def generate_csv(filename="clientes.csv"):
    filepath = os.path.join(BASE_DIR, filename)

    data = [
        {"id": "1", "nome": "Ana", "idade": "28", "renda": "3200"},
        {"id": "2", "nome": "Bruno", "idade": "35", "renda": "4500"},
        {"id": "3", "nome": "Carlos", "idade": "22", "renda": "2100"},
        {"id": "4", "nome": "Daniela", "idade": "40", "renda": "5000"},
    ]
    fieldnames = ["id", "nome", "idade", "renda"]

    with open(filepath, "w", newline="", encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    print(f"[✔] CSV gerado em: {filepath}")

def generate_db(filename="banco.db"):
    db_path = os.path.join(BASE_DIR, filename)

    if os.path.exists(db_path):
        os.remove(db_path)  # recria do zero sempre

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE credit_scores (
        id INTEGER PRIMARY KEY,
        score INTEGER,
        status TEXT
    )
    """)

    sample_data = [
        (1, 700, "aprovado"),
        (2, 500, "rejeitado"),
        (3, 620, "pendente"),
        (4, 780, "aprovado")
    ]

    cursor.executemany("INSERT INTO credit_scores (id, score, status) VALUES (?, ?, ?)", sample_data)

    conn.commit()
    conn.close()

    print(f"[✔] Banco SQLite gerado em: {db_path}")

if __name__ == "__main__":
    generate_csv()
    generate_db()
