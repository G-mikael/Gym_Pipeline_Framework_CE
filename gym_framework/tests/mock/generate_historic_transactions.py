import random
import csv
from datetime import datetime, timedelta

def gerar_transacoes(qtd=1000, nome_arquivo='transacoes.txt'):
    moedas = ['BRL', 'USD', 'EUR']
    hoje = datetime.today()

    with open(nome_arquivo, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'cliente_id', 'data', 'valor', 'moeda'])

        for i in range(5000, 5000 + qtd):
            cliente_id = random.randint(1, 100)
            dias_atras = random.randint(0, 60)
            data = (hoje - timedelta(days=dias_atras)).strftime('%Y-%m-%d')
            valor = round(random.uniform(50, 5000), 2)
            moeda = random.choices(moedas, weights=[0.6, 0.3, 0.1])[0]

            writer.writerow([i, cliente_id, data, valor, moeda])

    print(f'{qtd} transações geradas em "{nome_arquivo}"')

# Exemplo de uso
gerar_transacoes(qtd=10000)



import csv
from datetime import datetime

def marcar_suspeitas(nome_arquivo='transacoes.txt', saida='transacoes_rotuladas.txt'):
    def is_suspeita(valor, moeda, data_str):
        data = datetime.strptime(data_str, '%Y-%m-%d')
        fim_de_semana = data.weekday() >= 5
        return valor > 3000 or moeda != 'BRL' or fim_de_semana

    with open(nome_arquivo, 'r') as fin, open(saida, 'w', newline='') as fout:
        reader = csv.DictReader(fin)
        fieldnames = reader.fieldnames + ['suspeita']
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            valor = float(row['valor'])
            moeda = row['moeda']
            data = row['data']
            row['suspeita'] = int(is_suspeita(valor, moeda, data))
            writer.writerow(row)

    print(f'Transações rotuladas com suspeita foram salvas em "{saida}"')

marcar_suspeitas()

