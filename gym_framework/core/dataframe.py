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
        
        rows_number = len(self.data)
        columns_number = len(self.columns)
        
        return [rows_number, columns_number]
    
    def add_column(self, column_name : str, new_column_data : list):
        try:
            if len(new_column_data) != self.shape()[1]:
                raise ValueError("O tamanho da nova coluna não corresponde ao número de linhas.")
            
            self.columns.append(column_name)
            for item, row in zip(new_column_data, self.data):
                row[column_name] = item

        except Exception as e:
            print(f"Erro ao adicionar coluna: {e}")
            
    def add_row(self, data):
        return True
    