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
        self._running = False  # Controle de estado
        self._pipeline_active = True  # Sincronização com pipeline
        
        self.Handlers = {
            'clients': ClientsDBProducerHandler,
            'transactions': TransactionsDBProducerHandler,
            'scores': ScoreCSVProducerHandler
        }

    def set_pipeline(self, pipeline):
        self.pipeline = pipeline
        self._pipeline_active = True

    def pipeline_stopped(self):
        """Método para ser chamado quando o pipeline para"""
        self._pipeline_active = False

    def stop(self):
        """Para o processamento dos buffers"""
        self._running = False

    def process_buffers(self):
        """Processa buffers apenas quando o pipeline está ativo"""
        print("Buffer processing started")
        self._running = True
        while self._running and self._pipeline_active:
            if not self.pipeline:
                time.sleep(1)
                continue

            # Verifica buffers dentro do lock
            with self.lock:
                if not any([self.clients_buffer, self.transactions_buffer, self.scores_buffer]):
                    time.sleep(1)
                    continue

                # Prepara dados para processamento
                buffers = {
                    'clients': self.clients_buffer.copy(),
                    'transactions': self.transactions_buffer.copy(),
                    'scores': self.scores_buffer.copy()
                }
                self.clients_buffer.clear()
                self.transactions_buffer.clear()
                self.scores_buffer.clear()

            # Processa fora do lock
            for buffer_type, data in buffers.items():
                if data:
                    try:
                        producer = self.Handlers[buffer_type]()
                        producer.data = data
                        self.pipeline.add_node(producer)
                        print(f"{len(data)} {buffer_type} adicionados ao pipeline")
                    except Exception as e:
                        print(f"Erro ao processar {buffer_type}: {e}")
                        # Recoloca os dados no buffer se houver erro
                        with self.lock:
                            if buffer_type == 'clients':
                                self.clients_buffer.extend(data)
                            elif buffer_type == 'transactions':
                                self.transactions_buffer.extend(data)
                            elif buffer_type == 'scores':
                                self.scores_buffer.extend(data)

            time.sleep(0.5)

        print("Buffer processing stopped")

def serve(pipeline):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service = EventIngestionService()
    service.set_pipeline(pipeline)
    event_ingestion_service_pb2_grpc.add_EventIngestionServiceServicer_to_server(service, server)
    server.add_insecure_port('[::]:50051')
    
    processing_thread = threading.Thread(target=service.process_buffers)
    processing_thread.start()
    
    server.start()
    print("Servidor RPC iniciado na porta 50051")
    return service, server, processing_thread