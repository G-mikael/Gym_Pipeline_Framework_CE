import time
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class TimerTrigger:
    def __init__(self, interval_seconds, extractor):
        self.interval = interval_seconds
        self.extractor = extractor
        self._running = False

    def _run(self):
        while self._running:
            print("[TimerTrigger] Executando extrator...")
            self.extractor.extract()
            time.sleep(self.interval)

    def start(self):
        self._running = True
        self.thread = threading.Thread(target=self._run)
        self.thread.start()

    def stop(self):
        self._running = False
        self.thread.join()

class FileTrigger(FileSystemEventHandler):
    def __init__(self, extractor, watch_path):
        self.extractor = extractor
        self.watch_path = watch_path

    def on_created(self, event):
        if event.is_directory:
            return
        print(f"[FileTrigger] Arquivo detectado: {event.src_path}")
        self.extractor.extract()

    def start(self):
        observer = Observer()
        observer.schedule(self, self.watch_path, recursive=False)
        observer.start()
        print(f"[FileTrigger] Monitorando: {self.watch_path}")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()