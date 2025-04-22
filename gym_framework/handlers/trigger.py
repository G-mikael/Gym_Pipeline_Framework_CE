from gym_framework.handlers.base_handler import HandlerNode
from gym_framework.core.pipeline import PipelineExecutor
from gym_framework.handlers.handler import *
from gym_framework.handlers.producer import *
from multiprocessing import Process
import os
from gym_framework.tests.mock.generate_data import gerar_arquivos_txt_simulados
from pathlib import Path
from typing import Optional


BASE_DIR = Path(__file__).parent.resolve()

class BaseTrigger:
    def __init__(self, handler_node: HandlerNode, interval: float = None):
        self.handler_node = handler_node
        self.interval = interval

    def start(self, pipeline: PipelineExecutor):
        raise NotImplementedError
    

class TimerTrigger(BaseTrigger):
    def __init__(self, handler_node, interval=5, max_runs: Optional[int] = None):
        super().__init__(handler_node, interval)
        self.max_runs = max_runs

    @staticmethod
    def trigger_loop(interval, max_runs, pipeline, handler_node):
        runs = 0
        pid = os.getpid()
        while max_runs is None or runs < max_runs:
            time.sleep(interval)
            pipeline.enqueue_producer(handler_node)
            runs += 1
            print(f"[TimerTrigger  | PID {pid}] Executou {runs} vez(es)")

    def start(self, pipeline: "PipelineExecutor"):
        pipeline.add_node(self.handler_node)
        p = Process(target=self.trigger_loop, args=(self.interval, self.max_runs, pipeline, self.handler_node))
        p.start()
        return p
    

class RequestTrigger(BaseTrigger):
    def __init__(self, handler_node, end = ".txt", watch_dir=BASE_DIR, poll_interval=2, max_checks=None):
        super().__init__(handler_node, interval=poll_interval)
        self.end = end
        self.watch_dir = watch_dir
        self.already_seen = set()
        self.max_checks = max_checks

    def start(self, pipeline: PipelineExecutor):
        pipeline.add_node(self.handler_node, True)
        p = Process(
            target=self.watch,
            args=(self.handler_node, self.end, pipeline, self.watch_dir, self.interval, self.max_checks)
        )
        p.start()
        return p

    @staticmethod
    def watch(handler_node, end, pipeline, watch_dir, poll_interval, max_checks=None):
        already_seen = set()
        pid = os.getpid()
        print(f"[RequestTrigger | PID {pid}] Observando diretório: {watch_dir}")
        
        checks = 0
        while True:
            if max_checks is not None and checks >= max_checks:
                print(f"[RequestTrigger | PID {pid}] Encerrando após {checks} ciclos.")
                break

            files = set(os.listdir(watch_dir))
            new_files = files - already_seen
            for fname in new_files:
                if fname.endswith(end):
                    print(f"[RequestTrigger | PID {pid}] Novo arquivo detectado: {fname}")
                    already_seen.add(fname)
                    pipeline.enqueue_producer(handler_node, data=os.path.join(watch_dir, fname))

            checks += 1
            time.sleep(poll_interval)


if __name__ == "__main__":
    print("Iniciando pipeline...")

    paralelo = True

    external_simulator_process = Process(target=gerar_arquivos_txt_simulados, args=(BASE_DIR,5,10000))
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


    start_time = time.perf_counter()

    # Inicia pipeline
    pipeline.start()

    end_time = time.perf_counter()
    elapsed = end_time - start_time

    print(f" Pipiline finalizou em {elapsed:.4f} segundos.")

    external_simulator_process.join()

    print("Pipeline finalizado.")
