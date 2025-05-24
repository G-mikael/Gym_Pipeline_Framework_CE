import grpc
from concurrent import futures
import threading
import time
import traceback
from gym_framework import event_ingestion_service_pb2
from gym_framework import event_ingestion_service_pb2_grpc
from gym_framework.handlers.base_handler import HandlerNode
from gym_framework.handlers.producer import (
    ClientsDBProducerHandler,
    TransactionsDBProducerHandler,
    ScoreCSVProducerHandler
)

class EventIngestionService(event_ingestion_service_pb2_grpc.EventIngestionServiceServicer):
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.clients_buffer = []
        self.transactions_buffer = []
        self.scores_buffer = []
        self.lock = threading.Lock()
        self._running = True
        
        # Mapeamento direto dos handlers
        self.handler_map = {
            'clients': self._get_handler(pipeline, 'ClientsDBProducer'),
            'transactions': self._get_handler(pipeline, 'TransactionsDBProducer'),
            'scores': self._get_handler(pipeline, 'ScoreCSVProducer')
        }
        
        # Validação dos handlers
        self._validate_handlers()

    @staticmethod
    def _get_handler(pipeline, node_name):
        """Obtém o handler de um nó do pipeline sem usar get_node"""
        for node in pipeline.nodes:
            print(f"Verificando node: {getattr(node, 'name', node)}")
            if getattr(node, 'name', None) == node_name:
                return node.handler
        return None
    
    def _validate_handlers(self):
        """Verifica se todos os handlers necessários foram encontrados"""
        for buffer_type, handler in self.handler_map.items():
            print(f"Validando handler para {buffer_type}: {handler}")
            if handler is None:
                raise ValueError(f"Handler para {buffer_type} não encontrado no pipeline")

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
        """Processa buffers e envia dados diretamente para os handlers"""
        while self._running:
            with self.lock:
                if self.clients_buffer:
                    self._send_to_handler('clients', self.clients_buffer)
                    self.clients_buffer = []
                
                if self.transactions_buffer:
                    self._send_to_handler('transactions', self.transactions_buffer)
                    self.transactions_buffer = []
                
                if self.scores_buffer:
                    self._send_to_handler('scores', self.scores_buffer)
                    self.scores_buffer = []
            
            time.sleep(0.5)

    def _send_to_handler(self, buffer_type, data):
        """Envia dados diretamente para o handler correspondente"""
        try:
            handler = self.handler_map.get(buffer_type)
            if handler and hasattr(handler, 'inject_data'):
                handler.inject_data(data)
                print(f"Enviados {len(data)} registros de {buffer_type}")
            else:
                print(f"Handler inválido para {buffer_type}")
        except Exception as e:
            print(f"Erro ao processar {buffer_type}: {str(e)}")
            traceback.print_exc()

    def stop(self):
        self._running = False

def serve(pipeline):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service = EventIngestionService(pipeline)
    event_ingestion_service_pb2_grpc.add_EventIngestionServiceServicer_to_server(service, server)
    server.add_insecure_port('[::]:50051')
    
    processing_thread = threading.Thread(target=service.process_buffers, daemon=True)
    processing_thread.start()
    
    server.start()
    print("Servidor RPC iniciado na porta 50051")
    return server, service