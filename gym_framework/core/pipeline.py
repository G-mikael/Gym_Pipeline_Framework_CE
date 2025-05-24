from multiprocessing import Process
from gym_framework.handlers.base_handler import HandlerNode
from gym_framework.handlers.handler import *
from gym_framework.handlers.producer import *
from multiprocessing import Queue
import threading
import queue  


class PipelineExecutor:
    def __init__(self, productores, nodes):
        self.productores = productores
        self.nodes = nodes
        self.node_queue = {node.name: Queue() for node in nodes}
        self.node_list = {node.name: node for node in nodes}
        self.rpc_queue = Queue()
        self.queue = Queue()
        self.processes = []

    def start(self, mode='filesystem'):
        # Inicia producers de arquivo se no modo filesystem
        if mode == 'filesystem':
            for productor in self.productores:
                p = Process(target=productor.run, args=(None, self.queue, self.node_queue))
                self.processes.append(p)
                p.start()
        
        # Loop principal
        if mode == 'rpc':
            while True:
                try:
                    while not self.rpc_queue.empty():
                        new_node = self.rpc_queue.get_nowait()
                        self.nodes.append(new_node)
                        print(f"Nó RPC recebido: {new_node.name}")
                except queue.Empty:
                    pass
            
                time.sleep(0.1)


    def enqueue_producer(self, handler_node: HandlerNode, data = None):
        name = handler_node.name
        self.queue.put(name)
        if data: self.node_queue[name].put(data)
    
    def _process_new_nodes(self):
        try:
            print(f"Tentando ler da node_queue (tamanho: {self.node_queue.qsize()})")  # DEBUG
            new_node = self.node_queue.get(timeout=5)  # Timeout de 5 segundos
            print(f"Nó recebido: {type(new_node)}")  # DEBUG
            self.nodes.append(new_node)
        except queue.Empty:
            print("node_queue vazia")  # DEBUG
            time.sleep(1)

    def get_node(self, node_name):
        """Retorna o nó com o nome especificado ou None se não encontrado"""
        for node in self.nodes:
            if node.name == node_name:
                return node
        return None
    
    def add_rpc_node(self, node): 
        """Adiciona nós recebidos via RPC"""
        self.rpc_queue.put(node)
    
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

class PipelineExecutor_rpc:
    def __init__(self, producers, nodes):
        self.producers = producers
        self.nodes = {node.name: node for node in nodes}
        self.processes = []
        self.running = True
        self.input_queue = Queue()
        self.output_queues = {node.name: Queue() for node in nodes}
        self.node_order = self._determine_execution_order()

    def _determine_execution_order(self):
        """Determina a ordem de execução baseada nas dependências"""
        order = []
        remaining_nodes = list(self.nodes.values())
        
        while remaining_nodes:
            added = False
            for node in remaining_nodes[:]:
                # Se não tem dependências ou todas as dependências já estão na ordem
                if not hasattr(node, 'dependencies') or all(dep in [n.name for n in order] for dep in node.dependencies):
                    order.append(node)
                    remaining_nodes.remove(node)
                    added = True
            
            if not added and remaining_nodes:
                raise ValueError("Circular dependency detected in pipeline nodes")
        
        return order
    
    def get_node(self, node_name):
        """Retorna o nó com o nome especificado ou None se não encontrado"""
        for node in self.nodes:
            if node.name == node_name:
                return node
        return None
    
    def add_node(self, node):
        """Adiciona um novo nó ao pipeline"""
        self.nodes[node.name] = node
        self.output_queues[node.name] = Queue()
        self.node_order = self._determine_execution_order()
        print(f"Added node: {node.name}")

    def start_producers(self):
        """Inicia todos os producers"""
        for producer in self.producers:
            p = Process(target=self._run_producer, args=(producer,))
            self.processes.append(p)
            p.start()

    def _run_producer(self, producer):
        """Executa um producer e coloca resultados na fila"""
        try:
            producer.execute()
            if hasattr(producer, '_output'):
                self.input_queue.put(producer._output)
        except Exception as e:
            print(f"Producer error: {e}")

    def process_nodes(self):
        """Processa todos os nós do pipeline na ordem correta"""
        while self.running:
            # Processa cada nó na ordem determinada
            for node in self.node_order:
                try:
                    # Pega dados da fila de entrada ou da fila do nó anterior
                    input_queue = self.input_queue if node == self.node_order[0] else self.output_queues[node.dependencies[0]]
                    input_data = input_queue.get_nowait()
                    
                    # Processa os dados
                    output_data = node.handler.handle(input_data)
                    
                    # Coloca na fila de saída para os próximos nós
                    self.output_queues[node.name].put(output_data)
                    
                except queue.Empty:
                    continue
                except Exception as e:
                    #print(f"Error in node {node.name}: {e}")
                    pass

            time.sleep(0.1)

    def run(self):
        """Executa o pipeline completo"""
        self.start_producers()
        
        try:
            # Thread para processar nós
            processing_thread = threading.Thread(target=self.process_nodes, daemon=True)
            processing_thread.start()
            
            # Mantém o pipeline rodando
            while self.running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Para o pipeline"""
        self.running = False
        for p in self.processes:
            if p.is_alive():
                p.terminate()
        print("Pipeline stopped")


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
