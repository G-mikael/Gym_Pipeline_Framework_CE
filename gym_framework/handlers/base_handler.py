from abc import ABC, abstractmethod
from multiprocessing import Pool, cpu_count
import time
import os
from gym_framework.core.dataframe import Dataframe


class PipelineContext:
    def __init__(self, queue, dependencies=None, pipeline_queue=None):
        self.queue = queue
        self.dependencies = dependencies or []
        self.pipeline_queue = pipeline_queue


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

    def message(self):
        pass


class HandlerNode:
    def __init__(self, name, handler: BaseHandler, dependencies=None, parallel=False, chunks=4):
        self.name = name
        self.handler = handler
        self.dependencies = dependencies or []
        self.parallel = parallel  # habilita paralelismo no handler
        self.chunks = chunks      # em quantas partes dividir

        self.output_nodes = []

        # Conecta as dependências
        for dep in self.dependencies:
            dep.add_dependent(self)

    def add_dependent(self, node):
        self.output_nodes.append((node.name, node))

    def send(self, context: PipelineContext, data):
        """
        Envia `data` para uma ou mais filas (se `queue` for uma lista).
        """
        for name, _ in context.dependencies:
            context.queue[name].put(data)
            context.pipeline_queue.put(name)
            print(f"\tEnviado para [{name}]") # Tirar print do handler

    def split_data(self, data):
        """Divide um Dataframe em chunks iguais."""
        if isinstance(data, Dataframe):
            total = len(data.data)
            chunk_size = max(1, total // self.chunks)
            chunks = [
                Dataframe(data.data[i:i + chunk_size], data.columns)
                for i in range(0, total, chunk_size)
            ]
            return chunks
        else:
            return [data]  # fallback: sem split


    def process_chunk(self, chunk):
        """Função auxiliar para aplicar o handler em cada chunk."""
        return self.handler.handle(chunk)

    def parallel_handle(self, data):
        chunks = self.split_data(data)
        with Pool(processes=min(cpu_count(), self.chunks)) as pool:
            results = pool.map(self.process_chunk, chunks)

        if isinstance(data, Dataframe):
            combined_data = []
            for df in results:
                combined_data.extend(df.data)
            return Dataframe(combined_data, data.columns)
        else:
            return results


    def run(self, node_queue = None, pipeline_queue = None, queue = None):
        pid = os.getpid()
        print(f"[{self.name} | PID {pid}] Iniciando...")

        if node_queue:
            data = node_queue.get()
        else:
            data = None

        self.handler.message()

        context = PipelineContext(queue, self.output_nodes, pipeline_queue)

        start_time = time.perf_counter()
        if self.parallel:
            result = self.parallel_handle(data)
        else:
            result = self.handler.handle(data)
        end_time = time.perf_counter()

        self.send(context, result)

        elapsed = end_time - start_time
        print(f"[{self.name}] Finalizou em {elapsed:.4f} segundos.")
