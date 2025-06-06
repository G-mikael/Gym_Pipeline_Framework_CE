# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: event_ingestion_service.proto
# Protobuf Python Version: 5.29.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    29,
    0,
    '',
    'event_ingestion_service.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x1d\x65vent_ingestion_service.proto\x12\x11gym_framework_rpc\"^\n\nClientData\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x0c\n\x04nome\x18\x02 \x01(\t\x12\x0b\n\x03\x63pf\x18\x03 \x01(\t\x12\x17\n\x0f\x64\x61ta_nascimento\x18\x04 \x01(\t\x12\x10\n\x08\x65ndereco\x18\x05 \x01(\t\"\x83\x01\n\x0fTransactionData\x12\n\n\x02id\x18\x01 \x01(\x03\x12\x12\n\ncliente_id\x18\x02 \x01(\x03\x12\x0c\n\x04\x64\x61ta\x18\x03 \x01(\t\x12\r\n\x05valor\x18\x04 \x01(\x01\x12\r\n\x05moeda\x18\x05 \x01(\t\x12\x16\n\tcategoria\x18\x06 \x01(\tH\x00\x88\x01\x01\x42\x0c\n\n_categoria\"v\n\tScoreData\x12\x0b\n\x03\x63pf\x18\x01 \x01(\t\x12\r\n\x05score\x18\x02 \x01(\x05\x12\x14\n\x0crenda_mensal\x18\x03 \x01(\x01\x12\x16\n\x0elimite_credito\x18\x04 \x01(\x01\x12\x1f\n\x17\x64\x61ta_ultima_atualizacao\x18\x05 \x01(\t\"v\n\x14IngestClientsRequest\x12,\n\x05items\x18\x01 \x03(\x0b\x32\x1d.gym_framework_rpc.ClientData\x12\x10\n\x08\x62\x61tch_id\x18\x02 \x01(\t\x12\x1e\n\x16\x65mission_timestamp_iso\x18\x03 \x01(\t\"\x80\x01\n\x19IngestTransactionsRequest\x12\x31\n\x05items\x18\x01 \x03(\x0b\x32\".gym_framework_rpc.TransactionData\x12\x10\n\x08\x62\x61tch_id\x18\x02 \x01(\t\x12\x1e\n\x16\x65mission_timestamp_iso\x18\x03 \x01(\t\"t\n\x13IngestScoresRequest\x12+\n\x05items\x18\x01 \x03(\x0b\x32\x1c.gym_framework_rpc.ScoreData\x12\x10\n\x08\x62\x61tch_id\x18\x02 \x01(\t\x12\x1e\n\x16\x65mission_timestamp_iso\x18\x03 \x01(\t\"\xaa\x01\n\x11IngestionResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\x12\x0f\n\x07message\x18\x02 \x01(\t\x12\x10\n\x08\x62\x61tch_id\x18\x03 \x01(\t\x12\x16\n\x0eitems_received\x18\x04 \x01(\x05\x12$\n\x1citems_processed_successfully\x18\x05 \x01(\x05\x12#\n\x1bprocessing_duration_seconds\x18\x06 \x01(\x02\x32\xbf\x02\n\x15\x45ventIngestionService\x12^\n\rIngestClients\x12\'.gym_framework_rpc.IngestClientsRequest\x1a$.gym_framework_rpc.IngestionResponse\x12h\n\x12IngestTransactions\x12,.gym_framework_rpc.IngestTransactionsRequest\x1a$.gym_framework_rpc.IngestionResponse\x12\\\n\x0cIngestScores\x12&.gym_framework_rpc.IngestScoresRequest\x1a$.gym_framework_rpc.IngestionResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'event_ingestion_service_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_CLIENTDATA']._serialized_start=52
  _globals['_CLIENTDATA']._serialized_end=146
  _globals['_TRANSACTIONDATA']._serialized_start=149
  _globals['_TRANSACTIONDATA']._serialized_end=280
  _globals['_SCOREDATA']._serialized_start=282
  _globals['_SCOREDATA']._serialized_end=400
  _globals['_INGESTCLIENTSREQUEST']._serialized_start=402
  _globals['_INGESTCLIENTSREQUEST']._serialized_end=520
  _globals['_INGESTTRANSACTIONSREQUEST']._serialized_start=523
  _globals['_INGESTTRANSACTIONSREQUEST']._serialized_end=651
  _globals['_INGESTSCORESREQUEST']._serialized_start=653
  _globals['_INGESTSCORESREQUEST']._serialized_end=769
  _globals['_INGESTIONRESPONSE']._serialized_start=772
  _globals['_INGESTIONRESPONSE']._serialized_end=942
  _globals['_EVENTINGESTIONSERVICE']._serialized_start=945
  _globals['_EVENTINGESTIONSERVICE']._serialized_end=1264
# @@protoc_insertion_point(module_scope)
