from gym_framework.handlers.base_handler import HandlerNode
from gym_framework.core.pipeline import PipelineExecutor
from gym_framework.handlers.handler import *
from gym_framework.handlers.producer import *
from multiprocessing import Process
import os

class BaseTrigger:
    def __init__(self, handler_node: HandlerNode, interval: float = None):
        self.handler_node = handler_node
        self.interval = interval

    def start(self, pipeline: "PipelineExecutor"):
        raise NotImplementedError
    
class TimerTrigger(BaseTrigger):
    @staticmethod
    def trigger_loop(interval, pipeline, handler_node):
        while True:
            time.sleep(interval)
            pipeline.enqueue_producer(handler_node)

    def start(self, pipeline: "PipelineExecutor"):
        pipeline.add_node(self.handler_node)
        p = Process(target=self.trigger_loop, args=(self.interval, pipeline, self.handler_node))
        p.start()
        return p
    
from pathlib import Path

BASE_DIR = Path(__file__).parent.resolve()

class RequestTrigger:
    def __init__(self, handler_node, end = ".txt", watch_dir=BASE_DIR, poll_interval=2):
        self.handler_node = handler_node
        self.end = end
        self.watch_dir = watch_dir
        self.poll_interval = poll_interval
        self.already_seen = set()

    def start(self, pipeline):
        pipeline.add_node(self.handler_node, True)
        p = Process(
            target=RequestTrigger.watch,
            args=(self.handler_node, self.end, pipeline, self.watch_dir, self.poll_interval)
        )
        p.start()
        return p

    @staticmethod
    def watch(handler_node, end, pipeline, watch_dir, poll_interval):
        already_seen = set()
        print(f"[RequestTrigger] Observando diretório: {watch_dir}")
        while True:
            files = set(os.listdir(watch_dir))
            new_files = files - already_seen
            for fname in new_files:
                if fname.endswith(end):
                    print(f"[RequestTrigger] Novo arquivo detectado: {fname}")
                    already_seen.add(fname)
                    pipeline.enqueue_producer(handler_node, data=os.path.join(watch_dir, fname))
            time.sleep(poll_interval)


if __name__ == "__main__":
    print("Iniciando pipeline...")

    # Nós apenas produtores
    score_produto_node = HandlerNode("ScoreCSVProducerHandler", ScoreCSVProducerHandler())
    client_produto_node = HandlerNode("ClientsDBProducerHandler", ClientsDBProducerHandler())
    transactions_produto_node = HandlerNode("TransactionsDBProducerHandler", TransactionsDBProducerHandler())
    new_transactions_produto_node = HandlerNode("NewTransactionsTXTProducerHandler", NewTransactionsTXTProducerHandler())
    trigger_transactions_produto_node = HandlerNode("TriggerTransactionsProducerHandler", TriggerTransactionsProducerHandler())

    # Restante dos nós
    transformador_node = HandlerNode("NormalizerNode", NormalizerHandler(), dependencies=[client_produto_node])
    loader_node = HandlerNode("LoaderNode", LoaderHandler(), dependencies=[transformador_node])
    classifier_node = HandlerNode("ClassifierHandler", ClassifierHandler(), dependencies=[new_transactions_produto_node, trigger_transactions_produto_node])
    save_node = HandlerNode("SaveToFileHandler", SaveToFileHandler(), dependencies=[classifier_node])
    calculete_node = HandlerNode("CalculateAverageGainHandler", CalculateAverageGainHandler(), dependencies=[classifier_node])

    # Executor
    pipeline = PipelineExecutor(
        [],
        [transformador_node, loader_node, classifier_node, save_node, calculete_node]
    )

    # Triggers
    trigger = TimerTrigger(trigger_transactions_produto_node, interval=3)
    trigger_process = trigger.start(pipeline)

    request_trigger_transactions_txt = RequestTrigger(new_transactions_produto_node)
    request_trigger_transactions_txt_process = request_trigger_transactions_txt.start(pipeline)

    request_trigger_score = RequestTrigger(score_produto_node, ".csv") 
    request_trigger_score_process = request_trigger_score.start(pipeline)

    request_trigger_client = RequestTrigger(client_produto_node, ".db")
    request_trigger_client_process = request_trigger_client.start(pipeline)

    request_trigger_transactions_db = RequestTrigger(transactions_produto_node, ".db")
    request_trigger_transactions_db_process = request_trigger_transactions_db.start(pipeline)


    # Inicia pipeline
    pipeline.start()

    print("Pipeline finalizado.")
