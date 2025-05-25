import subprocess
import sys
import os
import time

def run_clients(num_clients_to_run):
    """
    Lança um número especificado de instâncias do cliente gRPC.

    Args:
        num_clients_to_run (int): O número de instâncias do cliente a serem lançadas.
    """
    if num_clients_to_run < 1:
        print("O número de clientes deve ser pelo menos 1.")
        return

    client_script_module = "gym_framework.tests.grpc_client_simulator"
    output_dir = "load_test_logs"
    os.makedirs(output_dir, exist_ok=True)
    
    processes = []
    print(f"Lançando {num_clients_to_run} instâncias do cliente...")

    for i in range(num_clients_to_run):
        instance_num = i + 1
        log_file_path = os.path.join(output_dir, f"client_output_{instance_num}_of_{num_clients_to_run}.log")
        command = [sys.executable, "-m", client_script_module]
        
        print(f"  Iniciando instância {instance_num} de {num_clients_to_run}. Log: {log_file_path}")
        
        try:
            log_file = open(log_file_path, "w")
            process = subprocess.Popen(command, stdout=log_file, stderr=subprocess.STDOUT)
            processes.append((process, log_file))
        except Exception as e:
            print(f"Erro ao iniciar instância {instance_num} ou abrir log {log_file_path}: {e}")
            if log_file:
                log_file.close()
    
    print("\nEsperando todas as instâncias do cliente terminarem...")
    for process, log_file in processes:
        try:
            process.wait()
            print(f"Instância com PID {process.pid} (log: {log_file.name}) terminou com código {process.returncode}.")
        except Exception as e:
            print(f"Erro ao esperar pela instância com PID {process.pid} (log: {log_file.name}): {e}")
        finally:
            if log_file:
                log_file.close() 
    print(f"\nTodas as {len(processes)} instâncias do cliente terminaram.")
    print(f"Os arquivos de log foram salvos em: {os.path.abspath(output_dir)}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python run_load_test.py <numero_de_clientes>")
        sys.exit(1)
    
    try:
        num_clients = int(sys.argv[1])
        if num_clients <= 0:
            raise ValueError("O número de clientes deve ser um inteiro positivo.")
    except ValueError as e:
        print(f"Argumento inválido: {e}")
        print("Uso: python run_load_test.py <numero_de_clientes>")
        sys.exit(1)
        
    start_time = time.time()
    run_clients(num_clients)
    end_time = time.time()
    print(f"Tempo total para executar {num_clients} clientes: {end_time - start_time:.2f} segundos.") 