# Resumo das Alterações e Próximos Passos - Evolução gRPC

## Alterações Realizadas Hoje:

O foco de hoje foi estabelecer a camada de comunicação gRPC básica entre um simulador de dados (cliente) e um ponto de entrada de eventos (servidor), além de preparar o terreno para a integração com o pipeline ETL existente.

1.  **Definição do Contrato do Serviço (Protocol Buffers):**
    *   Criado o arquivo `gym_framework/protos/event_ingestion_service.proto`.
    *   Definidas as estruturas das mensagens para os dados que serão trafegados:
        *   `ClientData` (para dados de clientes)
        *   `TransactionData` (para dados de transações)
        *   `ScoreData` (para dados de scores)
        *   `IngestionResponse` (para respostas do servidor)
    *   Definido o serviço gRPC `EventIngestionService` com os métodos RPC:
        *   `IngestClient(ClientData) returns (IngestionResponse)`
        *   `IngestTransaction(TransactionData) returns (IngestionResponse)`
        *   `IngestScore(ScoreData) returns (IngestionResponse)`

2.  **Geração de Código gRPC (Stubs):**
    *   Utilizado o `grpc_tools.protoc` para gerar os arquivos Python a partir do `.proto`:
        *   `gym_framework/event_ingestion_service_pb2.py`: Contém as classes Python para as mensagens.
        *   `gym_framework/event_ingestion_service_pb2_grpc.py`: Contém o código do stub do cliente e a classe base do servidor.
    *   Resolvido um problema de importação no arquivo `_pb2_grpc.py` (modificando-o para usar importação relativa `from . import ...`) para garantir que funcione corretamente dentro da estrutura do pacote `gym_framework` ao ser executado com `python -m`.

3.  **Criação do Cliente gRPC (Simulador de Dados):**
    *   Criado o script `gym_framework/tests/grpc_client_simulator.py`.
    *   Este script é capaz de:
        *   Gerar dados simulados para clientes, transações e scores (lógica adaptada do `generate_data.py` original).
        *   Converter esses dados para as mensagens protobuf definidas.
        *   Conectar-se a um servidor gRPC (atualmente `localhost:50051`).
        *   Chamar os métodos RPC (`IngestClient`, `IngestTransaction`, `IngestScore`) para enviar os dados.
        *   Receber e imprimir a resposta do servidor.

4.  **Criação de um Servidor gRPC Simples (Dummy Server):**
    *   Criado o script `gym_framework/tests/simple_grpc_server.py`.
    *   Este servidor:
        *   Implementa os métodos do `EventIngestionService`.
        *   Ao receber uma chamada RPC, imprime os dados da mensagem recebida.
        *   Retorna uma `IngestionResponse` indicando sucesso e um `event_id` de exemplo.
        *   Serve como um "parceiro de teste" para o cliente gRPC, validando a comunicação básica.

5.  **Teste da Comunicação Básica:**
    *   Executamos o `simple_grpc_server.py` em um terminal e o `grpc_client_simulator.py` em outro.
    *   Confirmamos que o cliente consegue enviar dados para o servidor, e o servidor os recebe e responde corretamente.

## O Que Falta para Completar o Projeto (Conforme Descrição da Tarefa):

1.  **Fase 3: Integrar o Servidor gRPC com o Pipeline ETL:**
    *   **Criar o Servidor gRPC Principal (`gym_framework/rpc_server.py`):**
        *   Este servidor herdará de `EventIngestionServiceServicer`.
        *   Seus métodos (`IngestClient`, etc.) precisarão:
            *   Receber os dados da mensagem gRPC (`request`).
            *   Converter/transformar esses dados para o formato esperado pelos `ProducerHandler`s do pipeline (provavelmente um `Dataframe` ou lista de dicionários).
            *   Injetar esses dados no `PipelineExecutor` usando `pipeline_executor.enqueue_producer(target_producer_node, data=converted_data)`.
            *   O servidor precisará de uma referência à instância do `PipelineExecutor` e um mapeamento dos tipos de evento para os `HandlerNode`s produtores correspondentes.
    *   **Adaptar `ProducerHandler`s (se necessário):**
        *   Verificar se os `ProducerHandler`s existentes (ex: `NewTransactionsTXTProducerHandler`, `ClientsDBProducerHandler`, `ScoreCSVProducerHandler` em `gym_framework/handlers/producer.py`) podem aceitar dados diretamente (além de caminhos de arquivo). Se não, adaptá-los para que o método `handle(self, data)` possa processar os dados que virão do servidor gRPC.
    *   **Modificar Ponto de Entrada do ETL (`gym_framework/examples/main.py` ou similar):**
        *   Remover a inicialização dos `RequestTrigger`s baseados em arquivos para os fluxos que agora virão via RPC.
        *   Remover o simulador local de geração de arquivos (`external_simulator_process`).
        *   Instanciar o `PipelineExecutor` e os `HandlerNode`s como antes.
        *   Adicionar os `HandlerNode`s produtores que serão alimentados pelo RPC ao `PipelineExecutor` usando `pipeline.add_node(nome_do_no_produtor, queue=True)`.
        *   Iniciar o novo servidor gRPC (`rpc_server.py`) em um processo separado (usando `multiprocessing.Process`) para que ele rode em paralelo com o `pipeline.start()`.

2.  **Fase 4: Teste de Carga e Medição:**
    *   **Aprimorar Mecanismo de Rastreamento de Eventos (End-to-End):**
        *   **Cliente (`grpc_client_simulator.py`):** Ao enviar um evento, registrar de forma persistente (ex: arquivo CSV) o `event_id` (pode ser o retornado pelo servidor ou um gerado pelo cliente) e o `timestamp_envio`.
        *   **Servidor RPC Principal/Pipeline ETL:** Garantir que o `event_id` seja propagado junto com os dados através de todo o pipeline (ex: como uma coluna no `Dataframe`).
        *   **Handler de Finalização (ETL):** Um handler no final do pipeline deve registrar o `timestamp_finalizacao_analise` para cada `event_id` e salvar isso de forma persistente, correlacionando com o `timestamp_envio`.
    *   **Preparar o Cliente para Múltiplas Instâncias:**
        *   Modificar `grpc_client_simulator.py` para aceitar configurações via argumentos de linha de comando (ex: endereço do servidor, número de eventos, ID da instância do simulador, arquivo de log).
    *   **Execução do Experimento:**
        *   Executar o servidor ETL principal em uma máquina, dimensionando-o para usar toda a capacidade de processamento (o `gym_framework` já usa `multiprocessing`).
        *   Executar o `grpc_client_simulator.py` em múltiplas instâncias (de 1 a 20, em uma ou mais máquinas clientes), aumentando gradualmente.
        *   Coletar os dados de log dos timestamps de envio e finalização.
    *   **Análise dos Resultados:**
        *   Calcular a latência média (`timestamp_finalizacao_analise - timestamp_envio`) para cada `event_id`.
        *   Analisar como a latência média e o throughput (vazão) variam com o aumento do número de instâncias do simulador.
        *   Discutir os resultados e correlacionar com a arquitetura do pipeline.

## Instruções para Testar o Que Foi Feito Hoje:

O teste realizado hoje validou a comunicação básica entre o cliente gRPC e um servidor gRPC simples. Para repetir este teste:

1.  **Ambiente:**
    *   Abra dois terminais separados.
    *   Em ambos, navegue até o diretório raiz do projeto: `C:\\Users\\Cliente\\Gym_Pipeline_Framework_CE`.
    *   Em ambos, ative o ambiente virtual: `.\\venv\\Scripts\\Activate.ps1` (PowerShell) ou `.\\venv\\Scripts\\activate.bat` (CMD).
    *   (Opcional, mas recomendado se houver dúvidas sobre o `PYTHONPATH`) Em ambos, defina o `PYTHONPATH` para a raiz do projeto:
        *   PowerShell: `$env:PYTHONPATH = \"C:\\Users\\Cliente\\Gym_Pipeline_Framework_CE\"`
        *   CMD: `set PYTHONPATH=C:\Users\Cliente\Gym_Pipeline_Framework_CE`

2.  **Iniciar o Servidor gRPC Simples (Terminal 1):**
    ```bash
    python -m gym_framework.tests.simple_grpc_server
    ```
    *   **Saída Esperada:** `Servidor gRPC simples iniciado em localhost:50051...`
    *   Este terminal ficará ocupado executando o servidor.

3.  **Executar o Cliente gRPC Simulador (Terminal 2):**
    ```bash
    python -m gym_framework.tests.grpc_client_simulator
    ```
    *   **Saída Esperada no Cliente:**
        *   Mensagens de conexão bem-sucedida.
        *   Para cada evento enviado (cliente, transação, score): `... enviado: ... Resposta: Success=True, Msg='...', EventID='...'`
        *   Ao final: `Simulação concluída.` e `Canal gRPC fechado.`
    *   **Saída Esperada no Servidor (Terminal 1):**
        *   Para cada evento recebido: `[Servidor Simples] Recebido ...: ...`

4.  **Parar o Servidor:**
    *   No Terminal 1 (onde o servidor está rodando), pressione `Ctrl+C`.
    *   **Saída Esperada:** `Servidor gRPC simples parando...` e `Servidor parado.`

Este teste confirma que a definição do serviço `.proto`, a geração dos stubs, o cliente e o servidor simples estão se comunicando corretamente via gRPC. 