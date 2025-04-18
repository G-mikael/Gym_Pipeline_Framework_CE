from pathlib import Path
from gym_framework.sources.base_source import CSVSource, DBSource

BASE_DIR = Path(__file__).parent.resolve()
csv_path = BASE_DIR / "clientes.csv"
db_path = BASE_DIR / "banco.db"

print("\n=== Testando CSV_Source ===")
csv_source = CSVSource(csv_path)
df_csv = csv_source.get_extractor()
df_csv = df_csv.extract()
for row in df_csv.data:
    print(row)

print("\n=== Testando DB_Source ===")
query = "SELECT * FROM credit_scores"
db_source = DBSource(db_path, query)
df_db = db_source.get_extractor()
df_db = df_db.extract()
for row in df_db.data:
    print(row)