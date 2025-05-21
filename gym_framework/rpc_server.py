import grpc
from concurrent import futures
import time

from gym_framework import event_ingestion_service_pb2 as pb2
from gym_framework import event_ingestion_service_pb2_grpc as pb2_grpc


class GRPCEventIngestionServicer(pb2_grpc.EventIngestionServiceServicer):
    def __init__(self, pipeline_executor, handler_mapping):
        self.pipeline_executor = pipeline_executor
        self.handler_mapping = handler_mapping

    def IngestClient(self, request, context):
        data = {
            'id': request.id,
            'nome': request.nome,
            'cpf': request.cpf,
            'data_nascimento': request.data_nascimento,
            'endereco': request.endereco
        }
        event_id = request.cpf  # Pode ser ID também
        self.pipeline_executor.enqueue_producer(self.handler_mapping['client'], data)
        return pb2.IngestionResponse(success=True, message="Cliente processado", event_id=event_id)

    def IngestTransaction(self, request, context):
        data = {
            'id': request.id,
            'cliente_id': request.cliente_id,
            'data': request.data,
            'valor': request.valor,
            'moeda': request.moeda,
        }
        if request.HasField("categoria"):
            data['categoria'] = request.categoria

        event_id = str(request.id)
        self.pipeline_executor.enqueue_producer(self.handler_mapping['transaction'], data)
        return pb2.IngestionResponse(success=True, message="Transação processada", event_id=event_id)

    def IngestScore(self, request, context):
        data = {
            'cpf': request.cpf,
            'score': request.score,
            'renda_mensal': request.renda_mensal,
            'limite_credito': request.limite_credito,
            'data_ultima_atualizacao': request.data_ultima_atualizacao
        }
        event_id = request.cpf
        self.pipeline_executor.enqueue_producer(self.handler_mapping['score'], data)
        return pb2.IngestionResponse(success=True, message="Score processado", event_id=event_id)


def serve(pipeline_executor, handler_mapping, host='0.0.0.0', port=50051):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_EventIngestionServiceServicer_to_server(
        GRPCEventIngestionServicer(pipeline_executor, handler_mapping),
        server
    )
    server.add_insecure_port(f'{host}:{port}')
    server.start()
    print(f"[RPC Server] Servidor gRPC rodando em {host}:{port}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print("[RPC Server] Encerrando servidor gRPC...")
        server.stop(0)