import grpc
from concurrent import futures
from multiprocessing import Process
from gym_framework.core.pipeline import PipelineExecutor
from gym_framework.handlers.base_handler import HandlerNode
from gym_framework.handlers.handler import *
from gym_framework.handlers.metrics import *
from gym_framework.handlers.producer import *
from gym_framework.core.train_riskClassifierModel import treinar_e_salvar_modelo
import threading
from pathlib import Path
import time

from gym_framework import event_ingestion_service_pb2
from gym_framework import event_ingestion_service_pb2_grpc


class EventIngestionService(event_ingestion_service_pb2_grpc.EventIngestionServiceServicer):
    def __init__(self):
        self.clients_buffer = []
        self.transactions_buffer = []
        self.scores_buffer = []
        self.lock = threading.Lock()
        self.pipeline = None

    def set_pipeline(self, pipeline):
        self.pipeline = pipeline

    def IngestClient(self, request, context):
        client_data = {
            'id': request.id,
            'nome': request.nome,
            'cpf': request.cpf,
            'data_nascimento': request.data_nascimento,
            'endereco': request.endereco
        }
        with self.lock:
            self.clients_buffer.append(client_data)
        return event_ingestion_service_pb2.IngestionResponse(success=True, message="Client ingested")

    def IngestTransaction(self, request, context):
        transaction_data = {
            'id': request.id,
            'cliente_id': request.cliente_id,
            'data': request.data,
            'valor': request.valor,
            'moeda': request.moeda,
            'categoria': request.categoria if request.HasField('categoria') else None
        }
        with self.lock:
            self.transactions_buffer.append(transaction_data)
        return event_ingestion_service_pb2.IngestionResponse(success=True, message="Transaction ingested")

    def IngestScore(self, request, context):
        score_data = {
            'cpf': request.cpf,
            'score': request.score,
            'renda_mensal': request.renda_mensal,
            'limite_credito': request.limite_credito,
            'data_ultima_atualizacao': request.data_ultima_atualizacao
        }
        with self.lock:
            self.scores_buffer.append(score_data)
        return event_ingestion_service_pb2.IngestionResponse(success=True, message="Score ingested")

    def process_buffers(self):
        while True:
            if self.pipeline:
                with self.lock:
                    print(f"""
                            Buffer Status:
                            Clients: {len(self.clients_buffer)} items
                            Transactions: {len(self.transactions_buffer)} items
                            Scores: {len(self.scores_buffer)} items
                            """)
                    
                    # Process clients
                    if self.clients_buffer:
                        from gym_framework.handlers.handler import ClientsDBProducerHandler
                        producer = ClientsDBProducerHandler()
                        producer.data = self.clients_buffer.copy()
                        self.pipeline.add_node(producer)
                        self.clients_buffer.clear()
                    

                    # Process transactions
                    if self.transactions_buffer:
                        from gym_framework.handlers.handler import TransactionsDBProducerHandler
                        producer = TransactionsDBProducerHandler()
                        producer.data = self.transactions_buffer.copy()
                        self.pipeline.add_node(producer)
                        self.transactions_buffer.clear()

                    # Process scores
                    if self.scores_buffer:
                        from gym_framework.handlers.handler import ScoreCSVProducerHandler
                        producer = ScoreCSVProducerHandler()
                        producer.data = self.scores_buffer.copy()
                        self.pipeline.add_node(producer)
                        self.scores_buffer.clear()

            time.sleep(1)  # Check buffers

def serve(pipeline):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service = EventIngestionService()
    service.set_pipeline(pipeline)
    event_ingestion_service_pb2_grpc.add_EventIngestionServiceServicer_to_server(service, server)
    server.add_insecure_port('[::]:50051')
    
    # Start buffer processing thread
    processing_thread = threading.Thread(target=service.process_buffers, daemon=True)
    processing_thread.start()
    
    server.start()
    print("Servidor RPC iniciado na porta 50051")
    return server