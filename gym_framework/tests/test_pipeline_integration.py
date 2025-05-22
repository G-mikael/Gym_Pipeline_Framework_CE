from multiprocessing import Process
from gym_framework.rpc_server import serve
from gym_framework.core.pipeline import PipelineExecutor
from gym_framework.handlers.base_handler import HandlerNode
from gym_framework.handlers.handler import *
from gym_framework.handlers.metrics import *
from gym_framework.handlers.producer import *
from gym_framework.core.train_riskClassifierModel import treinar_e_salvar_modelo
from pathlib import Path
import threading
import time

BASE_DIR = Path(__file__).parent.resolve()
MOCKS_DIR = BASE_DIR / "mocks"

def setup_pipeline():
    # Configuração dos nós do pipeline (igual ao seu original, mas sem os producers de arquivo)
    paralelo = True

    # Nós de transformação e análise
    transformador_node = HandlerNode("NormalizerNode", NormalizerHandler(), parallel=paralelo)
    classifier_node = HandlerNode("ClassifierHandler", ClassifierHandler(), parallel=paralelo)
    risk_classifier_node = HandlerNode("RiskClassifierHandler", RiskTransactionClassifierHandler(), parallel=paralelo)
    save_node_csv = HandlerNode("SaveToCSVHandler", SaveToCSVHandler())
    calculete_node = HandlerNode("CalculateAverageGainHandler", CalculateAverageGainHandler())
    save_to_db_node = HandlerNode("SaveToDatabaseHandler", SaveToDatabaseHandler())
    calculate_most_transactions_node = HandlerNode("CalculateMostTransactionsHandler", CalculateMostTransactionsHandler())
    transaction_type_count_node = HandlerNode("TransactionTypeCountHandler", TransactionTypeCountHandler())
    risk_percentage_node = HandlerNode("RiskPercentageHandler", RiskPercentageHandler())
    currency_volume_node = HandlerNode("CurrencyVolumeHandler", CurrencyVolumeHandler())

    # Executor do pipeline
    pipeline = PipelineExecutor(
        [],
        [
            transformador_node, classifier_node, risk_classifier_node,
            save_node_csv, calculete_node, save_to_db_node,
            calculate_most_transactions_node, transaction_type_count_node,
            risk_percentage_node, currency_volume_node
        ]
    )
    return pipeline

if __name__ == "__main__":
    # 1. Configura o pipeline
    pipeline = setup_pipeline()
    
    # 2. Inicia o servidor RPC em uma thread separada
    server = serve(pipeline)
    
    # 3. Inicia o pipeline
    pipeline_thread = threading.Thread(target=pipeline.start)
    pipeline_thread.start()
    
    try:
        # Mantém o programa rodando
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDesligando o servidor...")
        server.stop(0)
        pipeline_thread.join()
        print("Servidor desligado.")