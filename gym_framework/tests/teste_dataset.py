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

print("Dataframe criado corretamente! \n")

print("Printando o conteúdo das 5 primeiras linhas do dataframe: \n")
print(df.showfirstrows(5), "\n")

print("Printando o conteúdo das 5 últimas linhas do dataframe: \n")
print(df.showlastrows(5), "\n")

print("Nome das colunas do dataframe: \n")
print(df.columns, "\n")

print("Utilizando a função get_column no dataframe: \n")
print(df.get_column("idade"), "\n")