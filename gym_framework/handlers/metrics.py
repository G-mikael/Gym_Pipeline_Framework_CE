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
        grouped = df.group_by("ClienteID")

        # Cria lista com (ID, quantidade de transações)
        counted = [{"ClienteID": id_, "count": len(group.data)} for id_, group in grouped.items()]

        # Ordena pela contagem em ordem decrescente
        sorted_counted = sorted(counted, key=lambda x: x["count"], reverse=True)

        # Pega o ID com mais transações
        top_id = sorted_counted[0]["ClienteID"]

        # Recupera os dados completos desse indivíduo (pega a primeira linha associada ao ID)
        top_individual = grouped[top_id].data[0]

        print(f"[CalculateMostTransactionsHandler] Indivíduo com mais transações: {top_individual}")
        return top_individual

    
