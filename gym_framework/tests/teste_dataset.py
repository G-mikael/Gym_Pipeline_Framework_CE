from .dataframe import Dataframe

df = Dataframe(
    [
    {"nome": "Alice", "idade": 25, "cidade": "São Paulo"},
    {"nome": "Bob", "idade": 30, "cidade": "Rio de Janeiro"},
]
)

print(df.showncolumns(2))