# Gym_Pipeline_Framework_CE

Aplicação prática dos conhecimentos adquiridos na matéria de Computação Escalável. Ao fim do projeto teremos um framework funcional "from scratch" e um projeto prático de demonstração.

## Descrição

Este projeto implementa um framework de **processamento de dados escalável**, com arquitetura baseada em nós `HandlerNode`, seguindo a lógica de produtor-consumidor. Ele permite a construção de pipelines com **extração, transformação, classificação, cálculo e persistência** de dados, de forma **modular e paralelizável**.

## ✅ Requisitos

- Python >= 3.7

Instale os requisitos com:

```bash
pip install -r requirements.txt
```

### 📦 Instalação do framework

Para instalar o framework de forma editável (útil durante o desenvolvimento), execute:

```bash
pip install -e .
```

> ⚠️ Certifique-se de rodar esse comando na **raiz do projeto**, onde está localizado o arquivo `setup.py`.

## Como Usar

### 1. Crie seus próprios Handlers

Você pode criar seus próprios handlers herdando de `BaseHandler`. Por exemplo:

```python
from gym_framework.handlers.base_handler import BaseHandler

class MeuHandler(BaseHandler):
    def handle(self, data):
        # lógica de processamento
        return data
```

### 2. Construa o pipeline com nós (`HandlerNode`)

Para montar um pipeline, você precisa criar **nós de processamento** usando os handlers implementados. Cada nó é uma instância de `HandlerNode`, que representa uma etapa do pipeline. Você também define as **dependências** entre os nós, ou seja, quem depende de quem para começar a processar.

Exemplo:

```python
from gym_framework.core.handler_node import HandlerNode
from gym_framework.core.pipeline import PipelineExecutor
from gym_framework.handlers import MeuHandler, OutroHandler

# Criação dos nós
inicio_node = HandlerNode("Inicio", MeuHandler())
meio_node = HandlerNode("Meio", OutroHandler(), dependencies=[inicio_node])
fim_node = HandlerNode("Fim", OutroHandler(), dependencies=[meio_node])

# Criação e execução do pipeline
pipeline = PipelineExecutor(producer_nodes=[inicio_node], consumer_nodes=[meio_node, fim_node])
pipeline.start()
```

- `dependencies` define de quais nós aquele nó depende.
- O pipeline executa os nós na ordem de acordo com essas dependências.

---


## 🧪 Exemplo Completo

Um exemplo completo de execução do pipeline com múltiplos produtores, transformadores, classificadores e salvadores de dados está disponível no arquivo:

```
examples/main.py
```

Esse exemplo simula um cenário prático com múltiplos tipos de fontes de dados, uso de triggers (`TimerTrigger`, `RequestTrigger`).

Para rodar o exemplo, basta executar:

```bash
python examples/main.py
```

O resultado será um pipeline funcional que consome dados de arquivos e banco de dados simulados, realiza transformações e classificações.

---

## 🧠 Modelo Entidade-Relacionamento do exemplo

![Diagrama ER](gym_framework/docs/er_model.png)

---

## 🔄 Fluxo do Pipeline de exemplo

![Fluxo do Pipeline](gym_framework/docs/pipeline_flow.png)

---

## 📁 Estrutura do Projeto

```
gym_framework/
├── core/          # Núcleo do framework (pipeline, dataframe, etc.)
├── docs/          # Documentos (diagramas, ER, etc.)
├── examples/      # Exemplo completo de uso do framework
│   ├── main.py    # Arquivo principal que executa o pipeline
├── extractors/    # Módulos de extração de dados
├── handlers/      # Handlers (processadores de dados)
├── loaders/       # Módulos de carregamento de dados   
├── mocks/         # Arquivos simulados utilizados durante os testes
setup.py           # Script de instalação do pacote
```

## 📈 Teste de Carga com gRPC

O sistema oferece um teste de carga para avaliar o desempenho do pipeline utilizando chamadas gRPC. Esse teste simula múltiplos clientes e mede o tempo médio de resposta (round-trip time) para diferentes níveis de concorrência.

### 🛠️ Pré-requisitos

* Python 3.x instalado
* Dependências instaladas com:

```bash
pip install -r requirements.txt
```

### 🚀 Etapas para Rodar o Teste de Carga

#### 1. Iniciar o Servidor gRPC

Abra um terminal e execute:

```bash
cd Gym_Pipeline_Framework_CE
python -m gym_framework.rpc_server
```

Esse comando iniciará o servidor gRPC que irá processar as requisições simuladas de carga. **Deixe este terminal aberto durante todo o teste.**

#### 2. Executar o Orquestrador do Teste de Carga

Em outro terminal:

```bash
cd Gym_Pipeline_Framework_CE
python orchestrate_load_test.py
```

Esse script realiza os seguintes passos automaticamente:

* Executa `run_load_test.py` para diferentes quantidades de clientes simuladores (conforme definido em `CLIENT_COUNTS_TO_TEST` dentro de `orchestrate_load_test.py`);
* Gera logs temporários em `load_test_logs/` para cada execução e os organiza posteriormente em `load_test_run_results/logs_<N>_clients/`;
* Chama `process_logs.py` para analisar os logs e calcular o tempo médio de round-trip para cada quantidade de clientes;
* Exibe os resultados no console.

## 👨‍💻 Autores

- Gabriel
- George
- Yoni 
- Mikael   
