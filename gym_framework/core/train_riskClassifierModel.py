import os
import pickle
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
from gym_framework.core.dataframe import Dataframe
import numpy as np
from datetime import datetime


MODELO_PATH = "models/risk_model.pkl"

def carregar_dados_rotulados(caminho_txt):
    # Carrega o dataset rotulado a partir de um arquivo CSV
    df = Dataframe.read_csv(caminho_txt)
    return df

def treinar_e_salvar_modelo(caminho_txt, caminho_modelo=MODELO_PATH):
    # Carrega os dados rotulados
    df_rotulado = carregar_dados_rotulados(caminho_txt)
    
    # Preprocessamento dos dados
    X_numerico = []
    moedas = []
    
    for row in df_rotulado.data:
        try:
            valor = float(row['Valor'])
            moeda = row['Moeda']
            data = datetime.strptime(row['Data'], '%Y-%m-%d')
            dia = data.weekday()
            mes = data.month
            X_numerico.append([valor, dia, mes])
            moedas.append([moeda])
        except Exception as e:
            print(f"Erro em linha: {e}")
            continue
    
    # Codificando a variável categórica 'Moeda'
    encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    moedas_cod = encoder.fit_transform(moedas)
    
    X = np.hstack([X_numerico, moedas_cod])
    y = np.array([int(row['Suspeita']) for row in df_rotulado.data])
    
    # Divisão entre treino e teste
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Criação e treinamento do modelo
    modelo = DecisionTreeClassifier(random_state=42)
    modelo.fit(X_train, y_train)
    
    # Avaliação no conjunto de teste
    y_pred = modelo.predict(X_test)
    print("===== AVALIAÇÃO NO CONJUNTO DE TESTE =====")
    print("Acurácia:", accuracy_score(y_test, y_pred))
    print("Relatório:\n", classification_report(y_test, y_pred))
    
    # Salvando o modelo e o encoder
    os.makedirs(os.path.dirname(caminho_modelo), exist_ok=True)
    with open(caminho_modelo, "wb") as f:
        pickle.dump({"modelo": modelo, "encoder": encoder}, f)
    
    print(f"Modelo treinado e salvo em: {caminho_modelo}")
