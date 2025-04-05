import time
import multiprocessing
from gym_framework.core.dataframe import Dataframe
from gym_framework.handlers.NormalizerHandler import NormalizerHandler
if __name__ == "__main__":
    # Função para testar o handler com diferentes quantidades de processos
    def test_with_processes(data, num_processes):
        df = Dataframe(data)
        handler = NormalizerHandler(num_processes=num_processes)
        
        start = time.time()
        normalized_df = handler.handle(df)
        end = time.time()
        
        print(f"\n🔧 Processos: {num_processes}")
        print(f"⏱ Tempo: {end - start:.4f} segundos")
        print("📄 Primeiras linhas:")
        print(normalized_df.showfirstrows(2))

    # Gerar 1.000.000 registros falsos
    print("🧪 Gerando dados simulados...")
    fake_data = [{"nome": f"Rua São João número {i}", "endereco": f"Avenida Brasil apartamento {i}"} for i in range(1000000)]

    # Testar com diferentes números de processos
    print("\n=== Teste de Desempenho - Normalização Paralela ===")

    test_with_processes(fake_data, 1)  # sem paralelismo
    test_with_processes(fake_data, 2)
    test_with_processes(fake_data, 4)
    test_with_processes(fake_data, multiprocessing.cpu_count())  # máximo disponível
