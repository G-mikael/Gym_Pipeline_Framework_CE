import csv

class Dataframe:
    def __init__(self, data=None, columns=None):
        """
        Fazer init do dataframe com uma uma lista de dicionários ou lista de listas
        """
        if data is None:
            self.data = []
            self.columns = columns or []
            
        #criar dataframe a partir de um dicionário
        elif isinstance(data, list) and all(isinstance(row, dict) for row in data):
            self.columns = list(data[0].keys()) #Adicionar as keys do primeiro item como colunas do dataframe
            self.data = data
        
        #criar dataframe com lista de listas de dados e uma lista de nomes para as colunas 
        elif isinstance(data, list) and all(isinstance(row, list) for row in data):
            if not columns:
                raise ValueError("Colunas devem ser fornecidas quando se usa uma matriz.")
            self.columns = columns
            self.data = [dict(zip(columns, row)) for row in data]
            
        else:
            raise TypeError("Formato de dados não suportado.")
    
    def showfirstrows(self, rows_number):
        """
        Mostrar as n primeiras linhas do dataframe
        """
        preview = [self.columns] + [[row[col] for col in self.columns] for row in self.data[:rows_number]]
        return "\n".join(str(row) for row in preview)
    
    def showlastrows(self, rows_number):
        """
        Mostrar as n últimas linhas do dataframe
        """
        preview = [self.columns] + [[row[col] for col in self.columns] for row in self.data[-rows_number:]]
        return "\n".join(str(row) for row in preview)
    
    def columns(self):
        """
        Lista todos os nomes das colunas do dataframe
        """
        return self.columns
    
    def get_column(self, column: str):
        """
        Listar itens de uma coluna específicada pelo usuário
        """
        if column not in self.columns:
            raise KeyError(f"Coluna '{column}' não encontrada.")
        return [row[column] for row in self.data]
    
    def shape(self):
        """
        Retorna o shape [m, n] do dataframe
        """
        rows_number = len(self.data)
        columns_number = len(self.columns)
        
        return [rows_number, columns_number]
    
    def add_column(self, column_name : str, new_column_data : list):
        """
        Adiciona uma nova coluna ao dataframe
        """
        try:
            if len(new_column_data) != self.shape()[0]:
                raise ValueError("O tamanho da nova coluna não corresponde ao número de linhas.")
            
            self.columns.append(column_name)
            for item, row in zip(new_column_data, self.data):
                row[column_name] = item

        except Exception as e:
            print(f"Erro ao adicionar coluna: {e}")
            
    def add_row(self, new_data):
        """
        Adiciona uma nova instância de dado (linha) ao dataframe
        """
        if len(new_data) != len(self.columns):
            raise ValueError("O tamanho da nova linha não corresponde ao número de colunas existentes.")
        
        new_roll = {}
        
        for new_value, key in zip(new_data, self.columns):
            new_roll[key] = new_value
            
        self.data.append(new_roll)
    
    def drop_column(self, column_name):
        """
        Remove uma coluna a partir do nome dela dado
        """
        if column_name not in self.columns:
            raise KeyError(f"Coluna '{column_name}' não encontrada.")
        self.columns.remove(column_name)
        for row in self.data:
            row.pop(column_name, None)

    def filter_rows(self, condition):
        """
        Retorna um dataframe com instâncias selecionadas a partir de uma condição especificada  
        """
        return Dataframe([row for row in self.data if condition(row)], self.columns)

    def select_columns(self, cols):
        """
        Retorna um novo dataframe com algumas colunas selecionadas
        """
        for col in cols:
            if col not in self.columns:
                raise KeyError(f"Coluna '{col}' não encontrada.")
        return Dataframe([{col: row[col] for col in cols} for row in self.data], cols)

    def rename_columns(self, rename_dict):
        """
        Renomeia as colunas do dataframe na forma {"nome_antigo": "nome_novo", "nome_antigo2": "nome_novo2"}
        """
        new_columns = [rename_dict.get(col, col) for col in self.columns]
        self.columns = new_columns
        for row in self.data:
            for old_col, new_col in rename_dict.items():
                if old_col in row:
                    row[new_col] = row.pop(old_col)

    def to_dict(self):
        """
        Retorna o conteúdo do dataframe no formato de lista de dicionários
        """
        return self.data.copy()

    def group_by(self, col):
        """
        Gera multiplos dataframes agrupados por diferentes valores de uma coluna especificada
        Os múltiplos dataframes são armazenados em um dicionario cujas chaves são os valores que estão agrupando os dados
        """
        if col not in self.columns:
            raise KeyError(f"Coluna '{col}' não encontrada.")
        groups = {}
        for row in self.data:
            key = row[col]
            groups.setdefault(key, []).append(row)
        return {k: Dataframe(v, self.columns.copy()) for k, v in groups.items()}

    def sort_by(self, col, reverse=False):
        """
        Ordena o dataframe em ordem crescente ou decrescente em ordem alfabética ou numérica dada uma coluna especificada
        """
        if col not in self.columns:
            raise KeyError(f"Coluna '{col}' não encontrada.")
        sorted_data = sorted(self.data, key=lambda row: row[col], reverse=reverse)
        return Dataframe(sorted_data, self.columns.copy())

    def merge(self, other, on):
        """
        Faz um inner join de dois dataframes em uma coluna especificada
        """
        if on not in self.columns or on not in other.columns:
            raise KeyError(f"Coluna '{on}' deve estar presente em ambos os dataframes.")
        merged_data = []
        for row in self.data:
            match = next((r for r in other.data if r[on] == row[on]), None)
            if match:
                new_row = row.copy()
                for key, value in match.items():
                    if key != on:
                        new_row[key] = value
                merged_data.append(new_row)
        merged_columns = list(set(self.columns + [col for col in other.columns if col != on]))
        return Dataframe(merged_data, merged_columns)

    def save_csv(self, path):
        """
        Salvar dataframe em um arquivo csv em um path especificado
        """
        with open(path, "w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.columns)
            writer.writeheader()
            writer.writerows(self.data)
    
    def apply_function_to_column(self, col, func):
        """
        Fazer cálculo matemático em certa coluna do dataframe
        """
        for row in self.data:
            if col in row:
                try:
                    val = float(row[col])
                    row[col] = func(val)
                except (ValueError, TypeError):
                    pass

    @staticmethod
    def read_csv(path):
        """
        Le um csv e retorna um dataframe
        """
        with open(path, "r", encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = [row for row in reader]
            columns = reader.fieldnames
        return Dataframe(data, columns)