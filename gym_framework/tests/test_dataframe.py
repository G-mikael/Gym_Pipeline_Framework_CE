from gym_framework.core.dataframe import Dataframe

print("Criando um df usando o módulo Dataframe: \n")

df = Dataframe(
    [
        {"nome": "Alice", "idade": 25, "cidade": "São Paulo"},
        {"nome": "Bob", "idade": 30, "cidade": "Rio de Janeiro"},
        {"nome": "Carla", "idade": 22, "cidade": "Belo Horizonte"},
        {"nome": "Diego", "idade": 28, "cidade": "Curitiba"},
        {"nome": "Elaine", "idade": 35, "cidade": "Fortaleza"},
        {"nome": "Fernando", "idade": 40, "cidade": "Porto Alegre"},
        {"nome": "Gabriela", "idade": 26, "cidade": "Recife"},
        {"nome": "Henrique", "idade": 31, "cidade": "Salvador"},
        {"nome": "Isabela", "idade": 29, "cidade": "Brasília"},
        {"nome": "João", "idade": 33, "cidade": "Manaus"},
    ]
)

df2 = Dataframe([
    ["Alice", 25, "São Paulo"],
    ["Bob", 30, "Rio de Janeiro"],
    ["Carla", 22, "Belo Horizonte"],
    ["Diego", 28, "Curitiba"],
    ["Elaine", 35, "Fortaleza"],
    ["Fernando", 40, "Porto Alegre"],
    ["Gabriela", 26, "Recife"],
    ["Henrique", 31, "Salvador"],
    ["Isabela", 29, "Brasília"],
    ["João", 33, "Manaus"],
], columns=["nome", "idade", "cidade"])

print("Dataframe criado corretamente! \n")

print("Printando o conteúdo das 5 primeiras linhas do dataframe: \n")
print(df.showfirstrows(5), "\n")
print(df2.showfirstrows(5), "\n")

print("Printando o conteúdo das 5 últimas linhas do dataframe: \n")
print(df.showlastrows(5), "\n")
print(df2.showlastrows(5), "\n")

print("Nome das colunas do dataframe: \n")
print(df.columns, "\n")
print(df2.columns, "\n")

print("Utilizando a função get_column no dataframe: \n")
print(df.get_column("idade"), "\n")
print(df2.get_column("cidade"), "\n")

print("Verificando formato do dataframe: \n")
print(df.shape(), "\n")

print("Adicionando nova coluna ao dataframe: \n")
df.add_column("Estado civíl", ["Casado", "Solteiro", "Casado", "Solteiro", "Casado", "Solteiro", "Casado", "Solteiro", "Casado", "Solteiro"])
print(df.showfirstrows(5), "\n")
