import grpc
from concurrent import futures
import uuid
import logging
import sys
import os
import time
import datetime
import pytz

if __name__ == '__main__' or __package__ is None or __package__ == '':
    current_dir_rpc = os.path.dirname(os.path.abspath(__file__))
    parent_dir_rpc = os.path.dirname(current_dir_rpc) # gym_framework
    project_root_rpc = os.path.dirname(parent_dir_rpc) # Raiz do projeto
    if parent_dir_rpc not in sys.path:
        sys.path.insert(0, parent_dir_rpc)
    if project_root_rpc not in sys.path:
        sys.path.insert(0, project_root_rpc) 
else:
    pass 

from gym_framework import event_ingestion_service_pb2
from gym_framework import event_ingestion_service_pb2_grpc
from gym_framework.core.pipeline import PipelineExecutor
from gym_framework.handlers.base_handler import HandlerNode 
from gym_framework.handlers.producer import ClientsDBProducerHandler, NewTransactionsTXTProducerHandler, ScoreCSVProducerHandler, TriggerTransactionsProducerHandler # Added Trigger for completeness
from gym_framework.handlers.handler import NormalizerHandler, ClassifierHandler, RiskTransactionClassifierHandler, SaveToDatabaseHandler
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

app_logger = logging.getLogger(__name__) 
app_logger.setLevel(logging.INFO)
class ContextLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        if 'extra' not in kwargs:
            kwargs['extra'] = {}
        kwargs['extra'].setdefault('batch_id', 'N/A')
        kwargs['extra'].setdefault('event_id', 'N/A')
        return msg, kwargs

logger = ContextLogger(app_logger, {})

if not app_logger.hasHandlers():
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s - BatchID: %(batch_id)s - EventID: %(event_id)s')
    ch.setFormatter(formatter)
    app_logger.addHandler(ch)
    app_logger.propagate = False 

SQLITE_DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "etl_database.db"))
MODEL_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "models", "risk_model.pkl"))
TRAINING_DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "tests", "mock", "transacoes_rotuladas.txt"))

os.makedirs(os.path.dirname(SQLITE_DB_PATH), exist_ok=True)

class EventIngestionServiceImpl(event_ingestion_service_pb2_grpc.EventIngestionServiceServicer):
    def __init__(self):
        self.risk_classifier_handler_instance = RiskTransactionClassifierHandler(model_path=MODEL_PATH)
        self.normalizer_handler_instance = NormalizerHandler()
        self.category_classifier_handler_instance = ClassifierHandler()
        logger.info("EventIngestionServiceImpl initialized.")

    def _proto_to_dict(self, proto_message):
        data = {}
        for field_descriptor in proto_message.DESCRIPTOR.fields:
            field_name = field_descriptor.name
            value = getattr(proto_message, field_name)
            data[field_name] = value
        return data

    def _execute_pipeline_for_batch(self, batch_request, producer_handler_class, transformer_handler_classes, loader_handler_class, data_type_name, loader_table_name):
        start_server_processing_time_monotonic = time.monotonic()
        request_received_time_utc = datetime.datetime.now(pytz.utc)

        batch_id = batch_request.batch_id if batch_request.batch_id else f"generated_batch_{str(uuid.uuid4())[:8]}"
        emission_timestamp_iso = batch_request.emission_timestamp_iso
        items_received = len(batch_request.items)
        items_processed_successfully = 0
        server_processing_duration_sec = 0.0

        log_context = {'batch_id': batch_id, 'event_id': 'BATCH_LIFECYCLE'}
        logger.info(f"Processing {items_received} {data_type_name} items. Emission_ts: {emission_timestamp_iso}", extra=log_context)

        emission_datetime_obj = None
        if emission_timestamp_iso:
            try:
                emission_datetime_obj = datetime.datetime.fromisoformat(emission_timestamp_iso)
            except ValueError:
                logger.warning(f"Formato inválido para emission_timestamp_iso: {emission_timestamp_iso}", extra=log_context)
                emission_datetime_obj = None

        if not items_received:
            logger.info(f"Empty batch received for {data_type_name}. Nothing to process.", extra=log_context)
            end_server_processing_time_monotonic = time.monotonic()
            server_processing_duration_sec = end_server_processing_time_monotonic - start_server_processing_time_monotonic
            return event_ingestion_service_pb2.IngestionResponse(
                success=True, message=f"{data_type_name} batch empty, nothing to process.",
                batch_id=batch_id, items_received=0, items_processed_successfully=0,
                processing_duration_seconds=server_processing_duration_sec
            )

        batch_data_list = []
        for item_proto in batch_request.items:
            item_dict = self._proto_to_dict(item_proto)
            item_dict["batch_id"] = batch_id
            item_dict["event_id"] = str(uuid.uuid4())
            batch_data_list.append(item_dict)

        producer_name = f"{data_type_name.lower()}_producer_{str(uuid.uuid4())[:4]}" # Short UUID for name
        
        if producer_handler_class == TriggerTransactionsProducerHandler:
            _producer_handler = producer_handler_class(batch_id_prefix=f"rpc_{data_type_name.lower()}_batch")
        else:
            _producer_handler = producer_handler_class() 

        current_producer_node = HandlerNode(name=producer_name, handler=_producer_handler)
        
        nodes_for_executor = [current_producer_node]
        last_node_in_chain = current_producer_node

        for i, TransformerCls in enumerate(transformer_handler_classes):
            transformer_node_name = f"{data_type_name.lower()}_transformer_{i}_{str(uuid.uuid4())[:4]}"
            if TransformerCls == RiskTransactionClassifierHandler:
                _transformer_handler = self.risk_classifier_handler_instance
            elif TransformerCls == NormalizerHandler:
                _transformer_handler = self.normalizer_handler_instance
            elif TransformerCls == ClassifierHandler:
                _transformer_handler = self.category_classifier_handler_instance
            else:
                _transformer_handler = TransformerCls() 
            
            transformer_node = HandlerNode(name=transformer_node_name, handler=_transformer_handler, dependencies=[last_node_in_chain])
            nodes_for_executor.append(transformer_node)
            last_node_in_chain = transformer_node

        loader_node_name = f"{data_type_name.lower()}_loader_{str(uuid.uuid4())[:4]}"
        _loader_handler = loader_handler_class(db_path=SQLITE_DB_PATH, table_name=loader_table_name)
        loader_node = HandlerNode(name=loader_node_name, handler=_loader_handler, dependencies=[last_node_in_chain])
        nodes_for_executor.append(loader_node)
        executor = PipelineExecutor(productores=[], nodes=nodes_for_executor, run_once=True) 
        executor.node_queue[current_producer_node.name].put(batch_data_list) 
        executor.queue.put(current_producer_node.name)
        try:
            logger.info(f"Starting PipelineExecutor for {data_type_name} batch.", extra=log_context)
            executor.run()
            logger.info(f"PipelineExecutor finished for {data_type_name} batch.", extra=log_context)
            items_processed_successfully = items_received 
            success_flag = True
            message = f"{data_type_name} batch (ID: {batch_id}) processed successfully."

        except Exception as e:
            logger.error(f"Error during pipeline execution for {data_type_name} batch (ID: {batch_id}): {e}", exc_info=True, extra=log_context)
            success_flag = False
            message = f"Error processing {data_type_name} batch (ID: {batch_id}): {e}"

        end_server_processing_time_monotonic = time.monotonic()
        server_processing_duration_sec = end_server_processing_time_monotonic - start_server_processing_time_monotonic

        total_latency_sec = -1.0
        if emission_datetime_obj:
            total_latency = request_received_time_utc - emission_datetime_obj
            total_latency_sec = total_latency.total_seconds()
            logger.info(f"Batch {batch_id} ({data_type_name}): ServerProcTime={server_processing_duration_sec:.4f}s, EmissionToSrvReceiptLatency={total_latency_sec:.4f}s", extra=log_context)
        else:
            logger.info(f"Batch {batch_id} ({data_type_name}): ServerProcTime={server_processing_duration_sec:.4f}s (Timestamp de emissão não disponível/inválido)", extra=log_context)

        return event_ingestion_service_pb2.IngestionResponse(
            success=success_flag, message=message,
            batch_id=batch_id, items_received=items_received, items_processed_successfully=items_processed_successfully,
            processing_duration_seconds=server_processing_duration_sec
        )

    def IngestClients(self, request: event_ingestion_service_pb2.IngestClientsRequest, context):
        return self._execute_pipeline_for_batch(
            batch_request=request,
            producer_handler_class=ClientsDBProducerHandler,
            transformer_handler_classes=[NormalizerHandler],
            loader_handler_class=SaveToDatabaseHandler,
            data_type_name="Client",
            loader_table_name="clients"
        )

    def IngestTransactions(self, request: event_ingestion_service_pb2.IngestTransactionsRequest, context):
        return self._execute_pipeline_for_batch(
            batch_request=request,
            producer_handler_class=NewTransactionsTXTProducerHandler, # Ou TransactionsDBProducerHandler se o formato RPC for o mesmo
            transformer_handler_classes=[ClassifierHandler, RiskTransactionClassifierHandler],
            loader_handler_class=SaveToDatabaseHandler,
            data_type_name="Transaction",
            loader_table_name="transactions"
        )

    def IngestScores(self, request: event_ingestion_service_pb2.IngestScoresRequest, context):
        return self._execute_pipeline_for_batch(
            batch_request=request,
            producer_handler_class=ScoreCSVProducerHandler,
            transformer_handler_classes=[], # Sem transformadores específicos para scores por enquanto
            loader_handler_class=SaveToDatabaseHandler,
            data_type_name="Score",
            loader_table_name="scores"
        )

def serve():
    if not os.path.exists(MODEL_PATH):
        logger.warning(f"Model file not found at {MODEL_PATH}. Attempting to train model...")
        try:
            from gym_framework.core.train_riskClassifierModel import treinar_e_salvar_modelo
            if not os.path.exists(TRAINING_DATA_PATH):
                logger.error(f"Training data not found at {TRAINING_DATA_PATH}. Cannot train model.")
            else:
                os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
                logger.info(f"Training model with data from: {TRAINING_DATA_PATH}")
                logger.info(f"Saving trained model to: {MODEL_PATH}")
                treinar_e_salvar_modelo(caminho_dados=TRAINING_DATA_PATH, caminho_modelo=MODEL_PATH)
                logger.info("Model training completed.")
                if not os.path.exists(MODEL_PATH):
                    logger.error("Model training ran, but model file still not found.")
        except ImportError as ie:
            logger.error(f"Could not import training module: {ie}. Model will not be trained automatically.")
        except Exception as e:
            logger.error(f"Failed to train model: {e}", exc_info=True)

    num_grpc_workers = os.cpu_count()
    if not num_grpc_workers or num_grpc_workers < 1:
        logger.warning(f"os.cpu_count() retornou {num_grpc_workers}. Usando fallback de 10 workers para o servidor gRPC.")
        num_grpc_workers = 10 # Fallback
    else:
        logger.info(f"Configurando o servidor gRPC com {num_grpc_workers} workers (baseado em os.cpu_count()).")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=num_grpc_workers))
    event_ingestion_service_pb2_grpc.add_EventIngestionServiceServicer_to_server(
        EventIngestionServiceImpl(), server
    )
    server.add_insecure_port('[::]:50051')
    logger.info("Starting gRPC server on port 50051...")
    server.start()
    logger.info("Server started.")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
        server.stop(0)
        logger.info("Server shutdown complete.")

if __name__ == '__main__':
    serve() 