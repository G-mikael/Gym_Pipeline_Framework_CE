import grpc
from concurrent import futures
import threading
import time
import traceback
from gym_framework.core.pipeline import PipelineExecutor_rpc
from gym_framework import event_ingestion_service_pb2
from gym_framework import event_ingestion_service_pb2_grpc
from gym_framework.handlers.base_handler import HandlerNode
from gym_framework.handlers.handler import *


class EventIngestionService(event_ingestion_service_pb2_grpc.EventIngestionServiceServicer):
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.clients_buffer = []
        self.transactions_buffer = []
        self.scores_buffer = []
        self.lock = threading.Lock()
        self._running = True
        
        # Configura os handlers de producer
        self.clients_handler = rpc_ClientsDBProducerHandler()
        self.transactions_handler = rpc_TransactionsDBProducerHandler()
        self.scores_handler = rpc_ScoreCSVProducerHandler()
        
        # Adiciona os handlers ao pipeline
        self._setup_pipeline_handlers()

    def _setup_pipeline_handlers(self):
        """Configura os handlers de producer no pipeline"""
        # Cria nós para os handlers
        clients_node = HandlerNode("ClientsDBProducer", self.clients_handler)
        transactions_node = HandlerNode("TransactionsDBProducer", self.transactions_handler)
        scores_node = HandlerNode("ScoreCSVProducer", self.scores_handler)
        
        # Adiciona ao pipeline
        self.pipeline.add_node(clients_node)
        self.pipeline.add_node(transactions_node)
        self.pipeline.add_node(scores_node)

    def IngestClient(self, request, context):
        with self.lock:
            self.clients_buffer.append({
                'id': request.id,
                'nome': request.nome,
                'cpf': request.cpf,
                'data_nascimento': request.data_nascimento,
                'endereco': request.endereco
            })
        return event_ingestion_service_pb2.IngestionResponse(
            success=True,
            message="Client data received",
            event_id=f"client_{request.id}"
        )

    def IngestTransaction(self, request, context):
        with self.lock:
            tx_data = {
                'id': request.id,
                'cliente_id': request.cliente_id,
                'data': request.data,
                'valor': request.valor,
                'moeda': request.moeda
            }
            if request.HasField('categoria'):
                tx_data['categoria'] = request.categoria
            self.transactions_buffer.append(tx_data)
        return event_ingestion_service_pb2.IngestionResponse(
            success=True,
            message="Transaction data received",
            event_id=f"tx_{request.id}"
        )

    def IngestScore(self, request, context):
        with self.lock:
            self.scores_buffer.append({
                'cpf': request.cpf,
                'score': request.score,
                'renda_mensal': request.renda_mensal,
                'limite_credito': request.limite_credito,
                'data_ultima_atualizacao': request.data_ultima_atualizacao
            })
        return event_ingestion_service_pb2.IngestionResponse(
            success=True,
            message="Score data received",
            event_id=f"score_{request.cpf}"
        )

    def process_buffers(self):
        """Processa buffers e envia dados para os handlers"""
        while self._running:
            with self.lock:
                # Processa clientes
                if self.clients_buffer:
                    self.clients_handler.inject_data(self.clients_buffer)
                    self.clients_buffer = []
                
                # Processa transações
                if self.transactions_buffer:
                    self.transactions_handler.inject_data(self.transactions_buffer)
                    self.transactions_buffer = []
                
                # Processa scores
                if self.scores_buffer:
                    self.scores_handler.inject_data(self.scores_buffer)
                    self.scores_buffer = []
            
            time.sleep(0.1)

    def stop(self):
        self._running = False
        self.clients_handler.shutdown()
        self.transactions_handler.shutdown()
        self.scores_handler.shutdown()

def start_rpc_server(pipeline):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service = EventIngestionService(pipeline)
    event_ingestion_service_pb2_grpc.add_EventIngestionServiceServicer_to_server(service, server)
    server.add_insecure_port('[::]:50051')
    
    # Inicia thread para processar buffers
    processing_thread = threading.Thread(target=service.process_buffers, daemon=True)
    processing_thread.start()
    
    server.start()
    print("Servidor RPC iniciado na porta 50051")
    return server, service

if __name__ == "__main__":
    # 1. Configura o pipeline básico (sem os producers ainda)
    pipeline = PipelineExecutor_rpc([], [])
    
    # 2. Primeiro adicionamos os producers (fontes de dados)
    producers = [
        HandlerNode("ClientsDBProducer", rpc_ClientsDBProducerHandler()),
        HandlerNode("TransactionsDBProducer", rpc_TransactionsDBProducerHandler()),
        HandlerNode("ScoreCSVProducer", rpc_ScoreCSVProducerHandler())
    ]
    
    for producer in producers:
        pipeline.add_node(producer)
    
    # 3. Adiciona nós de processamento
    processing_nodes = [
        HandlerNode("NormalizerNode", NormalizerHandler()),
        HandlerNode("ClassifierHandler", ClassifierHandler()),
        HandlerNode("RiskClassifierHandler", RiskTransactionClassifierHandler()),
        HandlerNode("SaveToCSVHandler", SaveToCSVHandler()),
        HandlerNode("CalculateAverageGainHandler", CalculateAverageGainHandler()),
        HandlerNode("SaveToDatabaseHandler", SaveToDatabaseHandler()),
    ]
    
    for node in processing_nodes:
        pipeline.add_node(node)
    
    # 4. Configura as dependências corretamente
    # Normalizer depende dos três producers
    pipeline.nodes["NormalizerNode"].dependencies = [
        "ClientsDBProducer", 
        "TransactionsDBProducer", 
        "ScoreCSVProducer"
    ]
    
    # Classifier depende do Normalizer
    pipeline.nodes["ClassifierHandler"].dependencies = ["NormalizerNode"]
    
    # RiskClassifier depende do Classifier
    pipeline.nodes["RiskClassifierHandler"].dependencies = ["ClassifierHandler"]
    
    # Nós de saída dependem do RiskClassifier
    pipeline.nodes["SaveToCSVHandler"].dependencies = ["RiskClassifierHandler"]
    pipeline.nodes["CalculateAverageGainHandler"].dependencies = ["RiskClassifierHandler"]
    pipeline.nodes["SaveToDatabaseHandler"].dependencies = ["RiskClassifierHandler"]
    
    # 4. Inicia o servidor RPC
    server, rpc_service = start_rpc_server(pipeline)
    
    # 5. Inicia o pipeline em uma thread separada
    pipeline_thread = threading.Thread(target=pipeline.run, daemon=True)
    pipeline_thread.start()
        
    try:
        # Mantém o programa rodando
        while True:
            time.sleep(1000)
    except KeyboardInterrupt:
        print("\nDesligando...")
        rpc_service.stop()
        pipeline.stop()
        server.stop(0)