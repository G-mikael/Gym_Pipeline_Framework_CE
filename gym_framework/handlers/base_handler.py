from abc import ABC, abstractmethod
from multiprocessing import Queue
import time


class BaseHandler(ABC):
    @abstractmethod
    def handle(self, data, queue=None, dependencies=None, pipeline_queue=None):
        """
        Método que deve ser implementado por todos os tratadores.

        Args:
            data: O dataframe ou estrutura de dados a ser transformado.

        Returns:
            Dados transformados.
        """
        self.send(data, queue, dependencies)
        pass

    def send(self, queue, data, dependencies = None):
        """
        Envia `data` para uma ou mais filas (se `queue` for uma lista).
        """
        if isinstance(dependencies, list):
            for name, q in dependencies:
                queue[name].put(data)
                print(f"\tEnviado para [{name}]")
        elif queue:
            queue.put(data)


class HandlerNode:
    def __init__(self, name, handler: BaseHandler, dependencies=None):
        self.name = name
        self.handler = handler
        self.dependencies = dependencies or []

        self.output_nodes = []

        # Conecta as dependências
        for dep in self.dependencies:
            dep.add_dependent(self)

    def add_dependent(self, node):
        self.output_nodes.append((node.name, node))

    def run(self, node_queue = None, pipeline_queue = None, queue = None):
        print(f"[{self.name}] Iniciando...")
        
        if node_queue:
            data = node_queue.get() 
        else:
            data = None

        start_time = time.perf_counter()
        self.handler.handle(data, queue, self.output_nodes, pipeline_queue)
        end_time = time.perf_counter()

        #print(f"{self.name} - Output Nodes----------{self.output_nodes}----------")

        # Mudar isso
        for name, dep_node in self.output_nodes:
            #print(f"{self.name} adicionando na fila de {name}")
            for i in range(1):
                pipeline_queue.put(name)

        elapsed = end_time - start_time
        print(f"[{self.name}] Finalizou em {elapsed:.4f} segundos.")
