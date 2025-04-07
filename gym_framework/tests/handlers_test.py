from multiprocessing import Manager
from typing import List, Dict
import csv
import os
import unicodedata
import re
from gym_framework.core.dataframe import Dataframe
from gym_framework.handlers.handler import NormalizerHandler, FilterHandler, ColumnSelectorHandler, AggregatorHandler, CSVLoaderHandler

print("Preparando dados para testes...")

data = [
    {"NOME": "joão silva", "IDADE": 25, "ENDERECO": "rua das flores"},
    {"NOME": "maria costa", "IDADE": 17, "ENDERECO": "avenida brasil"},
    {"NOME": "ana beatriz", "IDADE": 30, "ENDERECO": "praça central"},
    {"NOME": "carlos lima", "IDADE": 22, "ENDERECO": "rua das acácias"},
    {"NOME": "pedro henrique", "IDADE": 45, "ENDERECO": "avenida paulista"},
    {"NOME": "fernanda souza", "IDADE": 19, "ENDERECO": "praça da república"},
    {"NOME": "lucas martins", "IDADE": 16, "ENDERECO": "rua são joão"},
    {"NOME": "patrícia oliveira", "IDADE": 28, "ENDERECO": "avenida dom pedro"},
    {"NOME": "juliana", "IDADE": 33, "ENDERECO": "praça sete"},
    {"NOME": "daniel", "IDADE": 40, "ENDERECO": "rua da independência"},
]

df = Dataframe(data, ["NOME", "IDADE", "ENDERECO"])

### ========== TESTES ==========

print("\n=== Teste NormalizerHandler ===")
normalizer = NormalizerHandler()
df_norm = normalizer.handle(df)
print(df_norm.showfirstrows(5))

print("\n=== Teste FilterHandler (maiores de 18) ===")
filtro = FilterHandler(lambda row: row["IDADE"] > 18)
df_filtro = filtro.handle(df)
print(df_filtro.showfirstrows(5))

print("\n=== Teste ColumnSelectorHandler (somente NOME) ===")
selector = ColumnSelectorHandler(["NOME"])
df_nomes = selector.handle(df)
(df_nomes.showfirstrows(5))

print("\n=== Teste AggregatorHandler ===")
agg_data = [
    {"CATEGORIA": "A", "VALOR": 10},
    {"CATEGORIA": "B", "VALOR": 20},
    {"CATEGORIA": "A", "VALOR": 5},
]
df_agg = Dataframe(agg_data, ["CATEGORIA", "VALOR"])
aggregator = AggregatorHandler("CATEGORIA", "VALOR", sum)
df_aggregated = aggregator.handle(df_agg)
print(df_aggregated.showfirstrows(5))

print("\n=== Teste CSVLoaderHandler ===")
csv_loader = CSVLoaderHandler("teste_output.csv")
msg = csv_loader.handle(df)
print(msg)
if os.path.exists("teste_output.csv"):
    with open("teste_output.csv", "r", encoding="utf-8") as f:
        print(f"\nConteúdo de teste_output.csv:\n{f.read()}")

"""print("\n=== Teste DataFrameSinkHandler ===")
manager = Manager()
shared_dict = manager.dict()
sink = DataFrameSinkHandler(shared_dict, "saida_pipeline")
sink.handle(df)
print("\nConteúdo no dicionário compartilhado:")
print(shared_dict["saida_pipeline"])"""