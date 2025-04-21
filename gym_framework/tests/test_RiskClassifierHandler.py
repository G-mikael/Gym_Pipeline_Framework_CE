import csv
import random
from datetime import datetime, timedelta
from datetime import datetime
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import OneHotEncoder
import numpy as np

class RiskTransactionClassifier:
    def __init__(self):
        self.modelo = DecisionTreeClassifier()
        self.encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')

    def _preprocessar(self, linhas, treinar_encoder=False):
        X_numerico = []
        moedas = []
        for row in linhas:
            valor = float(row['valor'])
            moeda = row['moeda']
            data = datetime.strptime(row['data'], '%Y-%m-%d')
            dia = data.weekday()
            mes = data.month
            X_numerico.append([valor, dia, mes])
            moedas.append([moeda])

        if treinar_encoder:
            moedas_cod = self.encoder.fit_transform(moedas)
        else:
            moedas_cod = self.encoder.transform(moedas)

        return np.hstack([X_numerico, moedas_cod])

    def treinar(self, arquivo_rotulado):
        with open(arquivo_rotulado, 'r') as f:
            reader = csv.DictReader(f)
            linhas = list(reader)

        X = self._preprocessar(linhas, treinar_encoder=True)
        y = np.array([int(row['suspeita']) for row in linhas])
        self.modelo.fit(X, y)

    def classificar_novas(self, arquivo_novo):
        with open(arquivo_novo, 'r') as f:
            reader = csv.DictReader(f)
            novas = list(reader)

        X_novo = self._preprocessar(novas)
        predicoes = self.modelo.predict(X_novo)

        for row, pred in zip(novas, predicoes):
            row['suspeita'] = int(pred)

        return novas  

def gerar_dados_teste(nome_arquivo='novas_transacoes.txt', qtd=10):
    moedas = ['BRL', 'USD', 'EUR']
    hoje = datetime.today()

    with open(nome_arquivo, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'cliente_id', 'data', 'valor', 'moeda'])

        for i in range(10000, 10000 + qtd):
            cliente_id = random.randint(1, 100)
            data = hoje - timedelta(days=random.randint(0, 30))
            valor = round(random.uniform(10.0, 10000.0), 2)
            moeda = random.choice(moedas)

            writer.writerow([i, cliente_id, data.strftime('%Y-%m-%d'), valor, moeda])

# Exemplo de uso:
gerar_dados_teste(qtd=100)


classifier = RiskTransactionClassifier()
classifier.treinar("C:/Users/Yoni/Documents/GitHub/Gym_Pipeline_Framework_CE/gym_framework/tests/mock/transacoes_rotuladas.txt")
novas_classificadas = classifier.classificar_novas("novas_transacoes.txt")
# Salva o resultado em um novo arquivo
with open("novas_transacoes_classificadas.txt", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=novas_classificadas[0].keys())
    writer.writeheader()
    writer.writerows(novas_classificadas)