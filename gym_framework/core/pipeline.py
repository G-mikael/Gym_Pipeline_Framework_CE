from multiprocessing import Process
from gym_framework.handlers.base_handler import HandlerNode
from gym_framework.handlers.handler import *
from gym_framework.handlers.producer import *
from multiprocessing import Queue
import queue  
import time


class PipelineExecutor:
    def __init__(self, productores, nodes, run_once=False):
        self.productores = productores
        self.nodes = nodes
        self.node_queue = {node.name: Queue() for node in nodes}
        self.node_list = {node.name: node for node in nodes}
        self.queue = Queue()
        self.active_processes = {}
        self.run_once = run_once
        self.max_idle_default = 11

    def start(self):
        if self.run_once:
            self.run()
            return

        for productor in self.productores:
            p = Process(target=productor.run, args=(None, self.queue, self.node_queue))
            p.start()
            self.active_processes[p.pid] = p
        
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
        if self.run_once:
            self._run_once_mode()
        else:
            self._run_continuous_mode()

    def _cleanup_finished_processes(self):
        finished_pids = []
        for pid, proc in self.active_processes.items():
            if not proc.is_alive():
                proc.join(timeout=0.1)
                finished_pids.append(pid)
        
        for pid in finished_pids:
            del self.active_processes[pid]
        return len(finished_pids) > 0

    def _run_once_mode(self):
        while not self.queue.empty() or self.active_processes:
            self._cleanup_finished_processes()
            
            try:
                item_name = self.queue.get(block=True, timeout=0.1) 
                
                if item_name in self.node_list:
                    node_to_run = self.node_list[item_name]
                    input_data_queue_for_node = self.node_queue.get(node_to_run.name)

                    p = Process(target=node_to_run.run, args=(input_data_queue_for_node, self.queue, self.node_queue))
                    p.start()
                    self.active_processes[p.pid] = p
                else:
                    print(f"PipelineExecutor: Aviso - Nome do nó '{item_name}' não encontrado na node_list.")

            except queue.Empty:
                if not self.active_processes:
                    break
                time.sleep(0.05) 
            except Exception as e:
                print(f"PipelineExecutor (run_once): Erro ao processar item da fila: {e}")
                time.sleep(0.1)

        for pid, proc in list(self.active_processes.items()):
            if proc.is_alive():
                proc.join(timeout=1)
            if proc.is_alive():
                print(f"PipelineExecutor (run_once): Processo {pid} ainda vivo após join. Tentando terminar.")
                proc.terminate()
                proc.join(timeout=0.5)
            self._cleanup_finished_processes()

        print("PipelineExecutor: Modo run_once concluído.")

    def _run_continuous_mode(self):
        print("PipelineExecutor: Executando em modo contínuo.")
        idle_time = 0
        max_idle = self.max_idle_default
        
        while True:
            cleaned_any = self._cleanup_finished_processes()
            if cleaned_any:
                idle_time = 0
            
            try:
                item_name = self.queue.get(block=True, timeout=1)
                idle_time = 0

                if item_name in self.node_list:
                    node_to_run = self.node_list[item_name]
                    input_data_queue_for_node = self.node_queue.get(node_to_run.name)
                    
                    p = Process(target=node_to_run.run, args=(input_data_queue_for_node, self.queue, self.node_queue))
                    p.start()
                    self.active_processes[p.pid] = p
                else:
                    print(f"PipelineExecutor: Aviso (contínuo) - Nó '{item_name}' não encontrado.")

            except queue.Empty:
                idle_time += 1
                if not self.active_processes and idle_time >= max_idle:
                    print(f"PipelineExecutor: Ocioso por {idle_time}s sem tarefas ou processos ativos. Encerrando.")
                    break
                elif self.active_processes:
                    idle_time = 0 
                    time.sleep(0.1)
                elif idle_time < max_idle :
                    pass

        for pid, proc in list(self.active_processes.items()):
             if proc.is_alive():
                proc.join(timeout=1)
             if proc.is_alive():
                print(f"PipelineExecutor (contínuo): Processo {pid} ainda vivo. Terminando.")
                proc.terminate()
                proc.join(timeout=0.5)
        self._cleanup_finished_processes()
        print("PipelineExecutor: Modo contínuo finalizado.")




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
