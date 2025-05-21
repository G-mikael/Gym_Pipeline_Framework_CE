from multiprocessing import Process
from gym_framework.rpc_server import serve
from gym_framework.core.pipeline import PipelineExecutor
from gym_framework.handlers.base_handler import HandlerNode
from gym_framework.handlers.handler import *
from gym_framework.handlers.metrics import *
from gym_framework.handlers.producer import *
from gym_framework.core.train_riskClassifierModel import treinar_e_salvar_modelo
from pathlib import Path
import time

BASE_DIR = Path(__file__).parent.resolve()
MOCKS_DIR = BASE_DIR / "mocks"

if __name__ == "__main__":
    print("Iniciando pipeline via RPC...")

    paralelo = True

    # Nós produtores
    score_produto_node = HandlerNode("ScoreCSVProducerHandler", ScoreCSVProducerHandler())
    client_produto_node = HandlerNode("ClientsDBProducerHandler", ClientsDBProducerHandler())
    transactions_produto_node = HandlerNode("TransactionsDBProducerHandler", TransactionsDBProducerHandler())
    new_transactions_produto_node = HandlerNode("NewTransactionsTXTProducerHandler", NewTransactionsTXTProducerHandler())
    trigger_transactions_produto_node = HandlerNode("TriggerTransactionsProducerHandler", TriggerTransactionsProducerHandler())

    # Nós consumidores e intermediários
    transformador_node = HandlerNode("NormalizerNode", NormalizerHandler(), dependencies=[client_produto_node], parallel=paralelo)
    classifier_node = HandlerNode("ClassifierHandler", ClassifierHandler(), dependencies=[new_transactions_produto_node, trigger_transactions_produto_node], parallel=paralelo)
    risk_classifier_node = HandlerNode("RiskClassifierHandler", RiskTransactionClassifierHandler(), dependencies=[new_transactions_produto_node], parallel=paralelo)
    save_node_csv = HandlerNode("SaveToCSVHandler", SaveToCSVHandler(), dependencies=[risk_classifier_node])
    calculete_node = HandlerNode("CalculateAverageGainHandler", CalculateAverageGainHandler(), dependencies=[classifier_node])
    save_to_db_node = HandlerNode("SaveToDatabaseHandler", SaveToDatabaseHandler(), dependencies=[classifier_node])
    calculate_most_transactions_node = HandlerNode("CalculateMostTransactionsHandler", CalculateMostTransactionsHandler(), dependencies=[transactions_produto_node])
    transaction_type_count_node = HandlerNode("TransactionTypeCountHandler", TransactionTypeCountHandler(), dependencies=[classifier_node])    
    risk_percentage_node = HandlerNode("RiskPercentageHandler", RiskPercentageHandler(), dependencies=[risk_classifier_node])
    currency_volume_node = HandlerNode("CurrencyVolumeHandler", CurrencyVolumeHandler(), dependencies=[risk_classifier_node])

    # Inicializa pipeline com os nós
    pipeline = PipelineExecutor(
        [score_produto_node, client_produto_node, transactions_produto_node,
         new_transactions_produto_node, trigger_transactions_produto_node],
        [
            transformador_node, classifier_node, risk_classifier_node,
            save_node_csv, calculete_node, save_to_db_node,
            calculate_most_transactions_node, transaction_type_count_node,
            risk_percentage_node, currency_volume_node
        ]
    )

    # Mapeamento de handlers para RPC
    handler_mapping = {
        "score": "ScoreCSVProducerHandler",
        "client": "ClientsDBProducerHandler",
        "transaction": "TransactionsDBProducerHandler",
        "new_transaction": "NewTransactionsTXTProducerHandler",
        "trigger_transaction": "TriggerTransactionsProducerHandler"
    }

    # Processo RPC separado
    rpc_process = Process(target=serve, args=(pipeline, handler_mapping))
    rpc_process.start()

    # Inicia o pipeline (gerencia e despacha nós conforme dados chegam)
    start_time = time.perf_counter()
    pipeline.start()
    end_time = time.perf_counter()

    print(f"Pipeline executado em {end_time - start_time:.2f} segundos.")
    rpc_process.join()