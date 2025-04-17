from abc import ABC, abstractmethod
import time


class PipelineContext:
    def __init__(self, queue, dependencies=None, pipeline_queue=None):
        self.queue = queue
        self.dependencies = dependencies or []
        self.pipeline_queue = pipeline_queue


class BaseHandler(ABC):
    @abstractmethod
    def handle(self, data, context: PipelineContext):
        """
        Método que deve ser implementado por todos os tratadores.

        Args:
            data: O dataframe ou estrutura de dados a ser transformado.
            context: Objeto com [queue, dependencies, pipeline_queue]
        """
        self.send(data, context)

    def send(self, context: PipelineContext, data):
        """
        Envia `data` para uma ou mais filas (se `queue` for uma lista).
        """
        for name, _ in context.dependencies:
            context.queue[name].put(data)
            context.pipeline_queue.put(name)
            print(f"\tEnviado para [{name}]") # Tirar print do handler


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

        context = PipelineContext(queue, self.output_nodes, pipeline_queue)

        start_time = time.perf_counter()
        self.handler.handle(context, data)
        end_time = time.perf_counter()

        elapsed = end_time - start_time
        print(f"[{self.name}] Finalizou em {elapsed:.4f} segundos.")
