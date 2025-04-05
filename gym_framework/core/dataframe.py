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
    
    def get_column(self, column : str):
        """
        Listar itens de uma coluna específica
        """
        if isinstance(self.data[0], dict):
            if column not in self.columns:
                raise KeyError(f"Coluna '{column}' não encontrada.")
            return [row[column] for row in self.data]
        
        else:
            try:
                col_index = self.columns.index(column)
            except ValueError:
                raise KeyError(f"Coluna '{column}' não encontrada.")
            return [row[col_index] for row in self.data]
            