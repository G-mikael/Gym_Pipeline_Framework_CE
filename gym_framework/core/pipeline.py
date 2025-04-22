from multiprocessing import Process
from gym_framework.handlers.base_handler import HandlerNode
from gym_framework.handlers.handler import *
from gym_framework.handlers.producer import *
from multiprocessing import Queue
import queue  


class PipelineExecutor:
    def __init__(self, productores, nodes):
        self.productores = productores
        self.nodes = nodes
        self.node_queue = {node.name: Queue() for node in nodes}
        self.node_list = {node.name: node for node in nodes}
        self.queue = Queue()
        self.processes = []

    def start(self):
        for productor in self.productores:
            for i in range(1):
                p = Process(target=productor.run, args=(None, self.queue, self.node_queue))
                self.processes.append(p)
                p.start()
        
        self.run()

    def enqueue_producer(self, handler_node: HandlerNode, data = None):
        name = handler_node.name
        self.queue.put(name)
        if data: self.node_queue[name].put(data)

    def add_node(self, node, queue = False):
        """Adiciono um nó a lista de nós do pipeline

        Args:
            node (HandlerNode): Nó de um handler
            queue (bool, optional): Se o nó necessita de input ou não. Defaults to False.
        """
        self.node_list[node.name] = node
        if queue: self.node_queue[node.name] = Queue()

    def run(self):
        idle_time = 0
        max_idle = 11  # segundos de espera antes de desistir
        
        while True:
            for p in self.processes[:]:
                if not p.is_alive():
                    p.join()
                    self.processes.remove(p)
            
            try:
                item = self.queue.get(timeout=1)  # espera por até 1s
                idle_time = 0  # reset idle

                node = self.node_list[item]  
                input_queue = self.node_queue.get(node.name)  


                p = Process(target=node.run, args=(input_queue, self.queue, self.node_queue))
                self.processes.append(p)
                p.start()
            except queue.Empty:
                idle_time += 1
                if idle_time >= max_idle:
                    break

        for p in self.processes:
            p.join()
        print("FIM")




# if __name__ == "__main__":
#     print("Iniciando pipeline...")

#     score_produto_node = HandlerNode("ScoreCSVProducerHandler", ScoreCSVProducerHandler())
#     client_produto_node = HandlerNode("ClientsDBProducerHandler", ClientsDBProducerHandler())
#     transactions_produto_node = HandlerNode("TransactionsDBProducerHandler", TransactionsDBProducerHandler())
#     new_transactions_produto_node = HandlerNode("NewTransactionsTXTProducerHandler", NewTransactionsTXTProducerHandler())



#     transformador_node = HandlerNode("NormalizerNode", NormalizerHandler(), dependencies=[client_produto_node])
#     loader_node = HandlerNode("LoaderNode", LoaderHandler(), dependencies=[transformador_node])
#     classifier_node = HandlerNode("ClassifierHandler", ClassifierHandler(), dependencies=[new_transactions_produto_node])
#     risk_classifier_node = HandlerNode("RiskClassifierHandler", RiskTransactionClassifierHandler(), dependencies=[transactions_produto_node,new_transactions_produto_node])
#     save_node = HandlerNode("SaveToFileHandler", SaveToFileHandler(), dependencies=[classifier_node])
#     calculete_node = HandlerNode("CalculateAverageGainHandler", CalculateAverageGainHandler(), dependencies=[classifier_node])



#     pipeline = PipelineExecutor([score_produto_node, client_produto_node, transactions_produto_node, new_transactions_produto_node],
#                                 [transformador_node, loader_node, classifier_node, save_node, calculete_node])
#     pipeline.start()

#     print("Pipeline finalizado.")
