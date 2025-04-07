import multiprocessing
from multiprocessing import Process, Queue, Manager

class PipelineExecutor:
    def __init__(self, nodes):
        self.nodes = nodes

    def run(self):
        processes = []
        for node in self.nodes:
            p = Process(target=node.run)
            p.start()
            processes.append(p)

        for p in processes:
            p.join()

class BalancedPipelineExecutor:
    def __init__(self, handlers_config, max_workers=None):
        self.handlers_config = handlers_config
        self.max_workers = max_workers or multiprocessing.cpu_count()

    def run(self):
        manager = Manager()
        result_dict = manager.dict()

        num_stages = len(self.handlers_config)
        queues = [Queue() for _ in range(num_stages + 1)]
        processes = []
        used_workers = 0

        for i, (handler_class, num_workers) in enumerate(self.handlers_config):
            actual_workers = min(num_workers, self.max_workers - used_workers)
            used_workers += actual_workers

            for _ in range(actual_workers):
                handler = handler_class(result_dict) if handler_class.__name__ == "LoaderHandler" else handler_class()
                p = Process(target=self._worker, args=(handler, queues[i], queues[i+1] if i+1 < len(queues) else None))
                p.start()
                processes.append(p)

        queues[0].put(None)  # Início do pipeline

        # Sinal de parada
        for q in queues:
            for _ in range(self.max_workers):
                q.put("STOP")

        for p in processes:
            p.join()

        return result_dict.get("saida")

    @staticmethod
    def _worker(handler, input_queue, output_queue):
        while True:
            item = input_queue.get()
            if item == "STOP":
                break
            result = handler.handle(item)
            if output_queue:
                output_queue.put(result)
