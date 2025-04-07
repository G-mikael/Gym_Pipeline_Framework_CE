from gym_framework.handlers.base_handler import BaseHandler
import csv
import time
import unicodedata
import re
import multiprocessing
from gym_framework.core.dataframe import Dataframe
from gym_framework.utils.logger import get_logger

logger = get_logger(__name__)

class NormalizerHandler(BaseHandler):
    def __init__(self, num_processes=None):
        self.num_processes = num_processes or multiprocessing.cpu_count()
        print(f"Using {self.num_processes} processes for normalization.")

    def normalize_text(self, text):
        text = unicodedata.normalize('NFD', text)
        text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
        text = text.upper()
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def simple_replace(self, text):
        substitutions = {
            "RUA": "R.",
            "AVENIDA": "AV.",
            "PRAÇA": "PÇA."
        }
        for original, replacement in substitutions.items():
            text = text.replace(original, replacement)
        return text

    def normalize_row(self, row):
        new_row = row.copy()
        for key, value in new_row.items():
            if isinstance(value, str):
                value = self.normalize_text(value)
                value = self.simple_replace(value)
                new_row[key] = value
        return new_row

    def handle(self, df: Dataframe) -> Dataframe:
        normalized_data = [self.normalize_row(row) for row in df.data]
        return Dataframe(normalized_data, df.columns)

class LoaderHandler(BaseHandler):
    def handle(self, data):
        time.sleep(1)
        print("Carregando dados...")
        print("Resultado final:", data[0])
        print(data[0].showfirstrows(10))

        print("3--------------------")

        return True

class FilterHandler(BaseHandler):
    def __init__(self, condition_func):
        self.condition_func = condition_func  # Função que recebe uma linha (dict) e retorna True/False

    def handle(self, df: Dataframe) -> Dataframe:
        filtered_data = [row for row in df.data if self.condition_func(row)]
        return Dataframe(filtered_data, df.columns)

class ColumnSelectorHandler(BaseHandler):
    def __init__(self, selected_columns):
        self.selected_columns = selected_columns

    def handle(self, df: Dataframe) -> Dataframe:
        filtered_data = [{col: row[col] for col in self.selected_columns if col in row} for row in df.data]
        return Dataframe(filtered_data, self.selected_columns)

class SinkHandler(BaseHandler):
    def __init__(self, output_dict=None, name="resultado"):
        self.output_dict = output_dict
        self.name = name

    def handle(self, df: Dataframe):
        if self.output_dict is not None:
            self.output_dict[self.name] = {
                "data": df.data,
                "columns": df.columns
            }
            print(f"[SinkHandler] Resultado armazenado em output_dict['{self.name}']")
        else:
            print("[SinkHandler] Nenhum dicionário de saída fornecido. Apenas imprimindo.")
            print(df.showfirstrows(10))
        return df

class CSVLoaderHandler(BaseHandler):
    def __init__(self, filepath="saida.csv"):
        self.filepath = filepath

    def handle(self, df: Dataframe):
        with open(self.filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=df.columns)
            writer.writeheader()
            writer.writerows(df.data)
        return f"Arquivo salvo em {self.filepath}"

class AggregatorHandler(BaseHandler):
    def __init__(self, group_by_column, agg_column, agg_func):
        self.group_by_column = group_by_column
        self.agg_column = agg_column
        self.agg_func = agg_func

    def handle(self, df: Dataframe) -> Dataframe:
        from collections import defaultdict

        grouped = defaultdict(list)
        for row in df.data:
            key = row[self.group_by_column]
            grouped[key].append(row[self.agg_column])

        result = [
            {self.group_by_column: key, self.agg_column: self.agg_func(values)}
            for key, values in grouped.items()
        ]

        return Dataframe(result, [self.group_by_column, self.agg_column])