from gym_framework.handlers.base_handler import HandlerNode
from gym_framework.core.pipeline import PipelineExecutor
from gym_framework.handlers.handler import *
from gym_framework.handlers.metrics import *
from gym_framework.handlers.producer import *
from gym_framework.handlers.trigger import TimerTrigger, RequestTrigger
from gym_framework.tests.mock.generate_data import gerar_arquivos_txt_simulados
from gym_framework.core.train_riskClassifierModel import treinar_e_salvar_modelo
from multiprocessing import Process
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve()
MOCKS_DIR = BASE_DIR / "mocks"


if __name__ == "__main__":
    # Treinamento antes do pipeline
    print("🔁 Treinando o modelo de risco com dados rotulados...")
    treinar_e_salvar_modelo("gym_framework/tests/mock/transacoes_rotuladas.txt")  
    print("Iniciando pipeline...")

    paralelo = True

    external_simulator_process = Process(target=gerar_arquivos_txt_simulados, args=(MOCKS_DIR,5,10000))
    external_simulator_process.start()

    # Nós apenas produtores
    score_produto_node = HandlerNode("ScoreCSVProducerHandler", ScoreCSVProducerHandler())
    client_produto_node = HandlerNode("ClientsDBProducerHandler", ClientsDBProducerHandler())
    transactions_produto_node = HandlerNode("TransactionsDBProducerHandler", TransactionsDBProducerHandler())
    new_transactions_produto_node = HandlerNode("NewTransactionsTXTProducerHandler", NewTransactionsTXTProducerHandler())
    trigger_transactions_produto_node = HandlerNode("TriggerTransactionsProducerHandler", TriggerTransactionsProducerHandler())

    # Restante dos nós
    transformador_node = HandlerNode("NormalizerNode",
                                     NormalizerHandler(),
                                     dependencies=[client_produto_node],
                                     parallel=paralelo)
    loader_node = HandlerNode("LoaderNode",
                              LoaderHandler(),
                              dependencies=[transformador_node])
    classifier_node = HandlerNode("ClassifierHandler",
                                  ClassifierHandler(),
                                  dependencies=[new_transactions_produto_node, trigger_transactions_produto_node],
                                  parallel=paralelo)
    risk_classifier_node = HandlerNode("RiskClassifierHandler", 
                                   RiskTransactionClassifierHandler(), 
                                   dependencies=[new_transactions_produto_node])
    save_node_csv = HandlerNode("SaveToCSVHandler",
                            SaveToCSVHandler(),
                            dependencies=[risk_classifier_node])
    calculete_node = HandlerNode("CalculateAverageGainHandler",
                                 CalculateAverageGainHandler(),
                                 dependencies=[classifier_node])
    save_to_db_node = HandlerNode("SaveToDatabaseHandler",
                                  SaveToDatabaseHandler(),
                                  dependencies=[classifier_node])
    # Adiciona o nó de métrica para calcular o número de transações mais frequentes
    calculate_most_transactions_node = HandlerNode("CalculateMostTransactionsHandler",
                                             CalculateMostTransactionsHandler(),
                                             dependencies=[transactions_produto_node])
    
    # Adiciona o nó de métrica para contar o número de transações por tipo
    transaction_type_count_node = HandlerNode("TransactionTypeCountHandler",
                                              TransactionTypeCountHandler(),
                                              dependencies=[classifier_node])  

    # Adiciona o nó de métrica para calcular a porcentagem de transações suspeitas
    risk_percentage_node = HandlerNode("RiskPercentageHandler",
                                                RiskPercentageHandler(),
                                                dependencies=[risk_classifier_node])

    # Executor
    pipeline = PipelineExecutor(
        [],
        [transformador_node, loader_node, classifier_node, save_node_csv, calculete_node, 
         risk_classifier_node, save_to_db_node, calculate_most_transactions_node, transaction_type_count_node, 
         risk_percentage_node]

    )
    

    # Triggers
    # trigger = TimerTrigger(trigger_transactions_produto_node, interval=3, max_runs=1)
    # trigger_process = trigger.start(pipeline)

    request_trigger_transactions_txt = RequestTrigger(new_transactions_produto_node, watch_dir=MOCKS_DIR)
    request_trigger_transactions_txt_process = request_trigger_transactions_txt.start(pipeline)

    # request_trigger_score = RequestTrigger(score_produto_node, ".csv", watch_dir=MOCKS_DIR) 
    # request_trigger_score_process = request_trigger_score.start(pipeline)

    # request_trigger_client = RequestTrigger(client_produto_node, ".db", watch_dir=MOCKS_DIR)
    # request_trigger_client_process = request_trigger_client.start(pipeline)

    # request_trigger_transactions_db = RequestTrigger(transactions_produto_node, ".db", watch_dir=MOCKS_DIR)
    # request_trigger_transactions_db_process = request_trigger_transactions_db.start(pipeline)


    start_time = time.perf_counter()

    # Inicia pipeline
    pipeline.start()

    end_time = time.perf_counter()
    elapsed = end_time - start_time

    print(f" Pipiline finalizou em {elapsed:.4f} segundos.")

    external_simulator_process.join()

    print("Pipeline finalizado.")
