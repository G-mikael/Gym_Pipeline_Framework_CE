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
    
    def showncolumns(self, rows_number):
        preview = [self.columns] + [[row[col] for col in self.columns] for row in self.data[:rows_number]]
        return "\n".join(str(row) for row in preview)