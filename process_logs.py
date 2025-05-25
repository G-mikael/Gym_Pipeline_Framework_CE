import os
import re
import sys
import statistics

def extract_round_trip_time(line):
    match = re.search(r"Round-trip=([\d\.]+)s", line)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None

def process_log_files(log_directory, verbose=True):
    """
    Processa todos os arquivos .log no diretório especificado, 
    extrai os tempos de Round-trip e calcula/retorna métricas.
    Retorna um dicionário com as métricas ou None se nenhum dado for encontrado.
    """
    all_round_trip_times = []
    num_log_files_processed = 0

    if not os.path.isdir(log_directory):
        if verbose:
            print(f"Erro: Diretório de logs não encontrado: {log_directory}")
        return None

    if verbose:
        print(f"Processando arquivos de log em: {os.path.abspath(log_directory)}")

    for filename in os.listdir(log_directory):
        if filename.endswith(".log"):
            log_file_path = os.path.join(log_directory, filename)
            file_round_trip_times = []
            try:
                with open(log_file_path, "r", encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        time_val = extract_round_trip_time(line)
                        if time_val is not None:
                            file_round_trip_times.append(time_val)
                
                if file_round_trip_times:
                    all_round_trip_times.extend(file_round_trip_times)
                    num_log_files_processed += 1
            except Exception as e:
                if verbose:
                    print(f"Erro ao ler ou processar o arquivo {log_file_path}: {e}")
    
    if not all_round_trip_times:
        if verbose:
            print("Nenhum tempo de Round-trip foi extraído de nenhum arquivo de log.")
        return None

    results = {
        "num_log_files_processed": num_log_files_processed,
        "total_samples": len(all_round_trip_times),
        "average_round_trip_time": statistics.mean(all_round_trip_times),
        "median_round_trip_time": statistics.median(all_round_trip_times),
        "stdev_round_trip_time": statistics.stdev(all_round_trip_times) if len(all_round_trip_times) > 1 else 0.0,
        "min_round_trip_time": min(all_round_trip_times),
        "max_round_trip_time": max(all_round_trip_times)
    }

    if verbose:
        print(f"\n--- Resultados Agregados para {log_directory} ---")
        print(f"Número total de arquivos de log processados: {results['num_log_files_processed']}")
        print(f"Número total de medições de Round-trip: {results['total_samples']}")
        print(f"Tempo médio de Round-trip: {results['average_round_trip_time']:.4f} segundos")
        print(f"Mediana do tempo de Round-trip: {results['median_round_trip_time']:.4f} segundos")
        print(f"Desvio padrão do tempo de Round-trip: {results['stdev_round_trip_time']:.4f} segundos")
        print(f"Tempo mínimo de Round-trip: {results['min_round_trip_time']:.4f} segundos")
        print(f"Tempo máximo de Round-trip: {results['max_round_trip_time']:.4f} segundos")
    
    return results

if __name__ == "__main__":
    default_log_dir = "load_test_logs"
    log_dir_to_process = default_log_dir

    if len(sys.argv) > 2:
        print("Uso: python process_logs.py [caminho_para_pasta_de_logs]")
        sys.exit(1)
    elif len(sys.argv) == 2:
        log_dir_to_process = sys.argv[1]
    
    process_log_files(log_dir_to_process, verbose=True)