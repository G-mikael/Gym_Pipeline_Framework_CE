from gym_framework.sources.db_source import Source
from gym_framework.extractors.extractor import CSV_Extractor

class CSVSource(Source):
    def __init__(self, filepath, name=None):
        super().__init__(name)
        self.extractor = CSV_Extractor(filepath)

    def read(self):
        return self.extractor.extract()