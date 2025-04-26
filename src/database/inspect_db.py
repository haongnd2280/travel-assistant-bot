import sqlite3
from pathlib import Path

import pandas as pd


db_file = Path(__file__).resolve().parent / "travel2.sqlite"

# Open the database file in read-only mode
conn = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True)

# List table names in the database
df = pd.read_sql(
    sql="SELECT name FROM sqlite_master WHERE type='table'",
    con=conn
)
# Convert table names to a list
tables = df.name.tolist()
print(tables)

for tb in tables:
    cursor = conn.execute(f"SELECT * FROM {tb}")
    rows = cursor.fetchall()
    counter = 0
    for row in rows:
        if counter > 10:
            break
        print(row)
        counter += 1

    # Returns metadata about the table
    cursor = conn.execute(f"PRAGMA table_info({tb});")
    columns = [row[1] for row in cursor.fetchall()]
    print(f"Columns in {tb}:")
    print(columns)


conn.close()
