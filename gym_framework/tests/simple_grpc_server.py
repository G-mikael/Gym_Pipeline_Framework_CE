from concurrent import futures
import grpc
import time
import uuid

from gym_framework import event_ingestion_service_pb2
from gym_framework import event_ingestion_service_pb2_grpc

class SimpleEventServicer(event_ingestion_service_pb2_grpc.EventIngestionServiceServicer):
    def IngestClient(self, request, context):
        print(f"[Servidor Simples] Recebido IngestClient: ID={request.id}, Nome='{request.nome}', CPF={request.cpf}")
        # Simula algum processamento
        time.sleep(0.01) 
        event_id = str(uuid.uuid4())
        return event_ingestion_service_pb2.IngestionResponse(
            success=True, 
            message=f"Cliente {request.id} recebido.",
            event_id=event_id
        )

    def IngestTransaction(self, request, context):
        print(f"[Servidor Simples] Recebido IngestTransaction: ID={request.id}, ClienteID={request.cliente_id}, Valor={request.valor}")
        # Simula algum processamento
        time.sleep(0.01)
        event_id = str(uuid.uuid4())
        return event_ingestion_service_pb2.IngestionResponse(
            success=True, 
            message=f"Transação {request.id} recebida.",
            event_id=event_id
        )

    def IngestScore(self, request, context):
        print(f"[Servidor Simples] Recebido IngestScore: ClienteCPF={request.cpf}, Score={request.score}")
        # Simula algum processamento
        time.sleep(0.01)
        event_id = str(uuid.uuid4())
        return event_ingestion_service_pb2.IngestionResponse(
            success=True, 
            message=f"Score para {request.cpf} recebido.",
            event_id=event_id
        )

def serve():
    server_address = 'localhost:50051'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    event_ingestion_service_pb2_grpc.add_EventIngestionServiceServicer_to_server(
        SimpleEventServicer(), server
    )
    server.add_insecure_port(server_address)
    print(f"Servidor gRPC simples iniciado em {server_address}...")
    server.start()
    try:
        while True:
            time.sleep(86400)  # Mantém o servidor rodando (um dia)
    except KeyboardInterrupt:
        print("Servidor gRPC simples parando...")
        server.stop(0)
        print("Servidor parado.")

if __name__ == '__main__':
    serve() 