from multiprocessing import Pool, Manager
from gym_framework.handlers.base_handler import *
from pathlib import Path
from gym_framework.sources.base_source import CSVSource, DBSource, TXTSource
from gym_framework.handlers.NormalizerHandler import NormalizerHandler
import time

import multiprocessing

class PipelineExecutor:
    def __init__(self, nodes):
        self.nodes = nodes
        self.processes = []

    def run(self):

        while(True):
            for node in self.nodes:
                p = multiprocessing.Process(target=run, args = (node,))
                self.processes.append(p)
                p.start()

            for p in self.processes:
                p.join()

def run(self: HandlerNode, data = None):
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
            dep_node.set_ready()

        return result

import pandas as pd

class ProdutoHandler(BaseHandler):
    def handle(self, data):
        """
        Método para tratar dados dos produtos.
        
        Args:
            data: Um DataFrame contendo informações dos produtos.
            
        Returns:
            DataFrame com as transformações aplicadas.
        """
        # Exemplo: Filtrar produtos com preço maior que 50
        import pandas as pd

        # Exemplo de DataFrame de produtos
        data = pd.DataFrame({
            'produto': ['Produto A', 'Produto B', 'Produto C'],
            'preco': [40, 60, 120],
            'quantidade': [10, 20, 30]
        })

        return data    
class TransformadorHandler(BaseHandler):
    def handle(self, data):
        """
        Método para transformar dados, normalizando os preços dos produtos.
        
        Args:
            data: Um DataFrame contendo informações dos produtos.
            
        Returns:
            DataFrame com os preços normalizados.
        """
        if isinstance(data, pd.DataFrame):
            # Normalização de preços
            max_preco = data['preco'].max()
            min_preco = data['preco'].min()
            data['preco_normalizado'] = (data['preco'] - min_preco) / (max_preco - min_preco)
            return data
        else:
            raise ValueError("O dado fornecido não é um DataFrame.")
class LoaderHandler(BaseHandler):
    def handle(self, data):
        """
        Método para carregar os dados em um arquivo CSV.
        
        Args:
            data: Um DataFrame contendo os dados a serem salvos.
            
        Returns:
            Uma mensagem indicando que os dados foram salvos com sucesso.
        """
        if isinstance(data, pd.DataFrame):
            data.to_csv('produtos_transformados.csv', index=False)
            return "Dados salvos com sucesso!"
        else:
            raise ValueError("O dado fornecido não é um DataFrame.")




if __name__ == "__main__":

    produto_handler = ProdutoHandler()
    transformador_handler = TransformadorHandler()
    loader_handler = LoaderHandler()

    print("Começo teste inicial")

    # Criando os nós
    extractor_node = HandlerNode("ProdutoNode", produto_handler)
    transformer_node = HandlerNode("TransformadorNode", transformador_handler, dependencies=[extractor_node])
    loader_node = HandlerNode("LoaderNode", loader_handler, dependencies=[transformer_node])

    print(extractor_node.input_queues)
    print(extractor_node.output_queues)
    print(transformer_node.input_queues)
    print(transformer_node.output_queues)
    print(loader_node.input_queues)
    print(loader_node.output_queues)
    

    # Pipeline
    pipeline = PipelineExecutor([extractor_node, transformer_node, loader_node])
    pipeline.run()

    print("Fim teste inicial")
