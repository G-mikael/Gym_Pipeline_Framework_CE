from abc import ABC, abstractmethod
from multiprocessing import Queue


class BaseHandler(ABC):
    @abstractmethod
    def handle(self, data):
        """
        Método que deve ser implementado por todos os tratadores.

        Args:
            data: O dataframe ou estrutura de dados a ser transformado.

        Returns:
            Dados transformados.
        """
        pass


class HandlerNode:
    def __init__(self, name, handler: BaseHandler, dependencies=None):
        self.name = name
        self.handler = handler
        self.dependencies = dependencies or []

        self.input_queues = []
        self.output_queues = []

        # Conecta as dependências
        for dep in self.dependencies:
            dep.add_dependent(self)

    def add_dependent(self, node):
        queue = Queue()
        self.output_queues.append((node, queue))
        node.input_queues.append(queue)

    def run(self):
        print(f"[{self.name}] Iniciando...")

        if self.input_queues:
            # Espera dados de todas as dependências
            inputs = [q.get() for q in self.input_queues]
            # Junta se tiver múltiplas entradas
            data = inputs if len(inputs) == 1 else inputs
        else:
            # Caso seja um nó de início, gera os dados
            data = None

        result = self.handler.handle(data)

        for _, queue in self.output_queues:
            queue.put(result)

        print(f"[{self.name}] Finalizou.")
