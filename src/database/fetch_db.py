import os
import sqlite3

import pandas as pd
import requests


def download_db_file(
    db_url: str,
    overwrite: bool = False,
    save_file: str = "travel2.sqlite",
) -> None:
    # The backup lets us restart for each tutorial section
    if not os.path.exists(save_file) or overwrite:
        print("Downloading database...")
        response = requests.get(db_url)
        response.raise_for_status()

        with open(save_file, "wb") as f:
            f.write(response.content)

        print("Database downloaded successfully.")
    else:
        print("Database already downloaded.")


def update_dates(fb_file: str) -> None:
    # Convert the flights to present time for our tutorial
    # We will use the backup file to update dates for the main db file

    download_db_file(
        db_url,
        save_file=fb_file,
    )

    conn = sqlite3.connect(fb_file)
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
    for tb in tables:
        dbs[tb] = pd.read_sql(f"SELECT * from {tb}", conn)

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


# ---------------------------------------
from pathlib import Path

db_url = "https://storage.googleapis.com/benchmarks-artifacts/travel-db/travel2.sqlite"
db_file = Path(__file__).resolve().parent / "travel2.sqlite"
