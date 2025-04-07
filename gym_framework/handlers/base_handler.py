from abc import ABC, abstractmethod
from queue import Queue
from multiprocessing import Lock, Event

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
        self.dependents = [] 
        self.input_queues = []
        self.output_queues = []

        for dep in self.dependencies:
            dep.add_dependent(self)

    def add_dependent(self, dependent_node):
        self.dependents.append(dependent_node)
        # Cria uma fila de output para este dependente
        q = Queue()
        self.output_queues.append((dependent_node, q))
        dependent_node.input_queues.append(q)

    def run(self, data = None):
        """O método para rodar o handler."""
        #self.ready_event.wait()  # Bloqueia até o evento ser setado
        print("Foi")
        if self.dependencies:
            inputs = [q.get() for q in self.input_queues]
        else:
            inputs = [data]


        result = self.handler.handle(inputs)

        # Envia para os dependentes
        for dep_node, q in self.output_queues:
            q.put(result)

        return result