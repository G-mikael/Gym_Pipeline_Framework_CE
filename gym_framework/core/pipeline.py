from multiprocessing import Process
from gym_framework.handlers.base_handler import HandlerNode
from gym_framework.handlers.handler import *
from gym_framework.handlers.producer import *



class PipelineExecutor:
    def __init__(self, nodes):
        self.nodes = nodes

    def run(self):
        processes = []
        for node in self.nodes:
            p = Process(target=node.run)
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

        print("FIM")


if __name__ == "__main__":
    print("Iniciando pipeline...")

    score_produto_node = HandlerNode("ScoreCSVProducerHandler", ScoreCSVProducerHandler())
    client_produto_node = HandlerNode("ClientsDBProducerHandler", ClientsDBProducerHandler())
    transactions_produto_node = HandlerNode("TransactionsDBProducerHandler", TransactionsDBProducerHandler())
    new_transactions_produto_node = HandlerNode("NewTransactionsTXTProducerHandler", NewTransactionsTXTProducerHandler())

    transformador_node = HandlerNode("NormalizerNode", NormalizerHandler(), dependencies=[client_produto_node])
    loader_node = HandlerNode("LoaderNode", LoaderHandler(), dependencies=[transformador_node])

    pipeline = PipelineExecutor([score_produto_node, client_produto_node, transactions_produto_node, new_transactions_produto_node, transformador_node, loader_node])
    pipeline.run()

    print("Pipeline finalizado.")
