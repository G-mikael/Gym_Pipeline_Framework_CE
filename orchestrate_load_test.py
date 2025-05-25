import subprocess
import sys
import os
import shutil
import time
import matplotlib.pyplot as plt
from process_logs import process_log_files 

# Configurações do Teste
CLIENT_COUNTS_TO_TEST = [1, 2, 5, 10, 15, 20]

BASE_RESULTS_DIR = "load_test_run_results"
TEMP_LOG_DIR_BY_RUNNER = "load_test_logs" 
CLIENT_RUNNER_SCRIPT = "run_load_test.py"

def run_single_test_configuration(num_clients, current_run_log_dir):
    print(f"\\n--- Iniciando teste para {num_clients} cliente(s) ---")
    if os.path.exists(TEMP_LOG_DIR_BY_RUNNER):
        print(f"Limpando diretório de logs temporário: {TEMP_LOG_DIR_BY_RUNNER}")
        try:
            shutil.rmtree(TEMP_LOG_DIR_BY_RUNNER)
        except OSError as e:
            print(f"Erro ao limpar {TEMP_LOG_DIR_BY_RUNNER}: {e}. Tentando continuar...")
    os.makedirs(TEMP_LOG_DIR_BY_RUNNER, exist_ok=True) 

    runner_command = [sys.executable, CLIENT_RUNNER_SCRIPT, str(num_clients)]
    print(f"Executando comando: {' '.join(runner_command)}")
    try:
        subprocess.run(runner_command, check=True, text=True, capture_output=False) 
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar {CLIENT_RUNNER_SCRIPT} para {num_clients} clientes: {e}")
        print("Output do erro:")
        print(e.stdout)
        print(e.stderr)
        return None # Falha nesta configuração
    except FileNotFoundError:
        print(f"Erro: Script {CLIENT_RUNNER_SCRIPT} não encontrado. Certifique-se que está no mesmo diretório.")
        sys.exit(1)

    print(f"Movendo logs de {TEMP_LOG_DIR_BY_RUNNER} para {current_run_log_dir}")
    os.makedirs(current_run_log_dir, exist_ok=True)
    files_to_move = os.listdir(TEMP_LOG_DIR_BY_RUNNER)
    if not files_to_move:
        print(f"Aviso: Nenhum arquivo de log encontrado em {TEMP_LOG_DIR_BY_RUNNER} após a execução para {num_clients} clientes.")
    for file_name in files_to_move:
        source_path = os.path.join(TEMP_LOG_DIR_BY_RUNNER, file_name)
        destination_path = os.path.join(current_run_log_dir, file_name)
        try:
            shutil.move(source_path, destination_path)
        except Exception as e:
            print(f"Erro ao mover {source_path} para {destination_path}: {e}")

    print(f"Processando logs em {current_run_log_dir}...")
    metrics = process_log_files(current_run_log_dir, verbose=False) 
    
    if metrics and 'average_round_trip_time' in metrics:
        avg_time = metrics['average_round_trip_time']
        print(f"Teste para {num_clients} cliente(s) concluído. Tempo médio de Round-trip: {avg_time:.4f}s")
        return avg_time
    else:
        print(f"Falha ao obter métricas para {num_clients} cliente(s) a partir de {current_run_log_dir}.")
        return None

def main():
    print("Iniciando orquestração do teste de carga...")
    os.makedirs(BASE_RESULTS_DIR, exist_ok=True)

    all_avg_times = []
    processed_client_counts = []

    print("\\nIMPORTANTE: Certifique-se que o servidor gRPC (rpc_server.py) está em execução em outro terminal!\n")
    time.sleep(3)
    for count in CLIENT_COUNTS_TO_TEST:
        run_specific_log_dir = os.path.join(BASE_RESULTS_DIR, f"logs_{count}_clients")
        avg_time = run_single_test_configuration(count, run_specific_log_dir)
        if avg_time is not None:
            all_avg_times.append(avg_time)
            processed_client_counts.append(count)
        else:
            print(f"Skipping client count {count} due to errors in test execution or log processing.")

    if not processed_client_counts or not all_avg_times:
        print("\\nNenhum resultado de teste foi coletado. Não é possível gerar o gráfico.")
        sys.exit(1)

    print("\\n--- Coleta de Dados Finalizada ---")
    for i, count in enumerate(processed_client_counts):
        print(f"Clientes: {count}, Tempo Médio Round-trip: {all_avg_times[i]:.4f}s")

    # Gerar o gráfico
    print("\\nGerando gráfico de performance...")
    try:
        plt.figure(figsize=(10, 6))
        plt.plot(processed_client_counts, all_avg_times, marker='o', linestyle='-')
        plt.title('Performance do Sistema ETL gRPC vs. Número de Clientes Concorrentes')
        plt.xlabel('Número de Clientes Simuladores Concorrentes')
        plt.ylabel('Tempo Médio de Round-trip (segundos)')
        plt.xticks(processed_client_counts) 
        plt.grid(True)
        
        graph_file_name = "load_test_performance_graph.png"
        plt.savefig(graph_file_name)
        print(f"Gráfico salvo como: {os.path.abspath(graph_file_name)}")
    except ImportError:
        print("\\nErro: Matplotlib não encontrado. Instale com 'pip install matplotlib' para gerar o gráfico.")
    except Exception as e:
        print(f"\\nErro ao gerar o gráfico: {e}")

    print("\\nOrquestração do teste de carga concluída.")

if __name__ == "__main__":
    main() 