from gym_framework.handlers.base_handler import BaseHandler
from gym_framework.core.dataframe import Dataframe


# Mostra o indivíduo com mais numero de transações

class CalculateMostTransactionsHandler(BaseHandler):
    """
    Classe para calcular o indivíduo com mais transações.
    """

    def handle(self, df):
        print("[CalculateMostTransactionsHandler] Calculando o indivíduo com mais transações...")

        # Agrupa por ID (retorna um dict: {id: DataFrame})
        grouped = df.group_by("cliente_id")

        # Cria lista com (ID, quantidade de transações)
        counted = [{"cliente_id": id_, "count": len(group.data)} for id_, group in grouped.items()]

        # Ordena pela contagem em ordem decrescente
        sorted_counted = sorted(counted, key=lambda x: x["count"], reverse=True)

        # Pega o ID com mais transações
        top_id = sorted_counted[0]["cliente_id"]

        # Recupera os dados completos desse indivíduo (pega a primeira linha associada ao ID)
        top_individual = grouped[top_id].data[0]

        print(f"[CalculateMostTransactionsHandler] Indivíduo com mais transações -> ClientID: {top_individual['cliente_id']} --- Número de transações:{sorted_counted[0]['count']}")
        return top_individual


class TransactionTypeCountHandler(BaseHandler):
    """
    Classe para contar a quantidade de cada tipo de transação.
    """
    def pre_message(self):
        print("[TransactionTypeCountHandler] Contando a quantidade de cada tipo de transação...")

    def handle(self, df):

        # Agrupa pelas categorias de tipo de transação
        grouped = df.group_by("categoria")

        # Conta quantas transações existem por tipo
        type_counts = [{"categoria": tipo, "quantidade": len(grupo.data)} for tipo, grupo in grouped.items()]

        return type_counts
        
    def pos_message(self, type_counts):
        # Exibe os resultados
        for tipo_count in type_counts:
            print(f"[TransactionTypeCountHandler] Tipo: {tipo_count['categoria']} --- Quantidade: {tipo_count['quantidade']}")
