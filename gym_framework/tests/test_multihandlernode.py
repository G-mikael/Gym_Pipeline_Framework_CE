from gym_framework.handlers.base_handler import HandlerNode, MultiHandlerNode
from gym_framework.core.pipeline import PipelineExecutor
from gym_framework.handlers.handler import *
from gym_framework.handlers.producer import *
from gym_framework.handlers.trigger import *
from multiprocessing import Process
import os
from gym_framework.tests.mock.generate_data import gerar_arquivos_txt_simulados
from pathlib import Path
from typing import Optional


if __name__ == "__main__":
    print("Iniciando pipeline...")

    paralelo = True

    external_simulator_process = Process(target=gerar_arquivos_txt_simulados, args=(BASE_DIR, 5, 10000))
    external_simulator_process.start()

    # Produtores
    score_produto_node = HandlerNode("ScoreCSVProducerHandler", ScoreCSVProducerHandler())
    client_produto_node = HandlerNode("ClientsDBProducerHandler", ClientsDBProducerHandler())
    transactions_produto_node = HandlerNode("TransactionsDBProducerHandler", TransactionsDBProducerHandler())
    new_transactions_produto_node = HandlerNode("NewTransactionsTXTProducerHandler", NewTransactionsTXTProducerHandler())
    trigger_transactions_produto_node = HandlerNode("TriggerTransactionsProducerHandler", TriggerTransactionsProducerHandler())

    # Transformadores
    transformador_node = HandlerNode("NormalizerNode",
                                     NormalizerHandler(),
                                     dependencies=[client_produto_node],
                                     parallel=paralelo)

    loader_node = HandlerNode("LoaderNode",
                              LoaderHandler(),
                              dependencies=[transformador_node])

    classifier_node = MultiHandlerNode("ClassifierHandler",
                                       ClassifierHandler(),
                                       dependencies=[new_transactions_produto_node, trigger_transactions_produto_node],
                                       parallel=paralelo)

    save_node = HandlerNode("SaveToFileHandler",
                            SaveToFileHandler(),
                            dependencies=[classifier_node])

    calculete_node = HandlerNode("CalculateAverageGainHandler",
                                 CalculateAverageGainHandler(),
                                 dependencies=[classifier_node])

    # Executor
    pipeline = PipelineExecutor(
        [],
        [transformador_node, loader_node, classifier_node, save_node, calculete_node]
    )

    # Triggers
    trigger = TimerTrigger(trigger_transactions_produto_node, interval=3, max_runs=20)
    trigger_process = trigger.start(pipeline)

    request_trigger_transactions_txt = RequestTrigger(new_transactions_produto_node)
    request_trigger_transactions_txt_process = request_trigger_transactions_txt.start(pipeline)

    request_trigger_score = RequestTrigger(score_produto_node, ".csv") 
    request_trigger_score_process = request_trigger_score.start(pipeline)

    request_trigger_client = RequestTrigger(client_produto_node, ".db")
    request_trigger_client_process = request_trigger_client.start(pipeline)

    request_trigger_transactions_db = RequestTrigger(transactions_produto_node, ".db")
    request_trigger_transactions_db_process = request_trigger_transactions_db.start(pipeline)

    # Roda a pipeline
    start_time = time.perf_counter()
    pipeline.start()
    end_time = time.perf_counter()

    print(f"Pipeline finalizou em {end_time - start_time:.4f} segundos.")

    external_simulator_process.join()
    print("Pipeline finalizado.")