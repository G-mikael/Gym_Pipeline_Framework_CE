from multiprocessing import Process
from gym_framework.handlers.base_handler import HandlerNode
from gym_framework.handlers.handler import *



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


if __name__ == "__main__":
    print("Iniciando pipeline...")

    produto_node = HandlerNode("ProdutoNode", ProdutoHandler())
    transformador_node = HandlerNode("TransformadorNode", TransformadorHandler(), dependencies=[produto_node])
    loader_node = HandlerNode("LoaderNode", LoaderHandler(), dependencies=[transformador_node])

    pipeline = PipelineExecutor([produto_node, transformador_node, loader_node])
    pipeline.run()

    print("Pipeline finalizado.")
