import os
import shutil
import sqlite3
from pathlib import Path

import pandas as pd
import requests


db_url = "https://storage.googleapis.com/benchmarks-artifacts/travel-db/travel2.sqlite"
db_file = Path(__file__).resolve().parent / "travel2.sqlite"
backup_file = Path(__file__).resolve().parent / "travel2.backup.sqlite"

def download_db_file(
    db_url: str = db_url,
    overwrite: bool = False,
    save_file: str = db_file,
    backup_file: str = backup_file,
) -> None:
    # The backup lets us restart for each tutorial section
    if not os.path.exists(save_file) or overwrite:
        print("Downloading database...")
        response = requests.get(db_url)
        response.raise_for_status()

        with open(save_file, "wb") as f:
            f.write(response.content)

        # The backup file should be created
        # the first time the database is downloaded
        shutil.copy(save_file, backup_file)

        print("Database downloaded successfully.")
    else:
        print("Database already downloaded.")


def update_dates(db_file: str) -> None:
    # Convert the flights to present time for our tutorial

    download_db_file(save_file=db_file)

    # We will use the backup file to reset the db in each section
    shutil.copy(backup_file, db_file)

    print("Updating dates in the database...")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Get the list of tables (`name` column)
    df = pd.read_sql(
        sql="SELECT name FROM sqlite_master WHERE type='table'",
        con=conn
    )
    
    # Convert table names to a list and fetch the tables
    tables = df.name.tolist()
    print(tables)

    # Reads each table into a pandas DataFrame and stores in a dict
    dbs = {}
    for table in tables:
        dbs[table] = pd.read_sql(f"SELECT * from {table}", conn)

    # Timestamp: Book date -> Schedule departure -> Actual departure -> Now

    # Finds the most recent actual departure time in the `flights` table
    # Ignores missing values marked as "\\N"
    dp_time = pd.to_datetime(
        dbs["flights"]["actual_departure"].replace("\\N", pd.NaT)
    )
    latest_time = dp_time.max()

    # Calculates how much time has passed since the latest actual departure
    current_time = pd.to_datetime("now").tz_localize(latest_time.tz)
    time_diff = current_time - latest_time

    # Shifts booking dates forward by the same `time_diff` so the data feels current
    book_date = pd.to_datetime(
        dbs["bookings"]["book_date"].replace("\\N", pd.NaT),
        utc=True
    )
    dbs["bookings"]["book_date"] = book_date + time_diff

    # Shifts all the datetime columns in the `flights` table forward by `time_diff`
    datetime_columns = [
        "scheduled_departure",
        "scheduled_arrival",
        "actual_departure",
        "actual_arrival",
    ]
    for column in datetime_columns:
        old_datetime = pd.to_datetime(
            dbs["flights"][column].replace("\\N", pd.NaT)
        )
        dbs["flights"][column] = old_datetime + time_diff

    # Writes the modified DataFrames back into the database
    for table_name, df in dbs.items():
        df.to_sql(
            table_name,
            conn,
            if_exists="replace",
            index=False
        )
        
    # Cleans up memory and commits changes to the DB
    del df
    del dbs
    conn.commit()
    conn.close()

    print("Database updated successfully.")
