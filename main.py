from time import time
import pymssql
from os import getenv

import pandas
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from dotenv import load_dotenv

TABLE_NAME = "Instances"

load_dotenv()

SOURCE_DB_HOST = getenv("SOURCE_DB_HOST")
SOURCE_DB_PORT = getenv("SOURCE_DB_PORT")
SOURCE_DB_NAME = getenv("SOURCE_DB_NAME")
SOURCE_DB_USER = getenv("SOURCE_DB_USER")
SOURCE_DB_PASSWORD = getenv("SOURCE_DB_PASSWORD")

DESTINATION_DB_HOST = getenv("DESTINATION_DB_HOST")
DESTINATION_DB_PORT = getenv("DESTINATION_DB_PORT")
DESTINATION_DB_NAME = getenv("DESTINATION_DB_NAME")
DESTINATION_DB_USER = getenv("DESTINATION_DB_USER")
DESTINATION_DB_PASSWORD = getenv("DESTINATION_DB_PASSWORD")


def connect_to_source_db():
    try:
        conn = pymssql.connect(SOURCE_DB_HOST, SOURCE_DB_USER, SOURCE_DB_PASSWORD, SOURCE_DB_NAME)
        return conn
    except Exception as err:
        print(f"Error connecting to Source DB. {err}")


def connect_to_destination_db():
    SQLALCHEMY_DATABASE_URL = f"postgresql+psycopg2://{DESTINATION_DB_USER}:{DESTINATION_DB_PASSWORD}@{DESTINATION_DB_HOST}:{DESTINATION_DB_PORT}/{DESTINATION_DB_NAME}"

    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    LocalSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    db = None
    try:
        db = LocalSession()
        return db
    except Exception as err:
        print(f"Error connecting to Destination DB. {err}")


def test_source(conn):
    conn.execute(f"SELECT * from {TABLE_NAME} WHERE Instance_ID BETWEEN {3291840} AND {3291846};")


# def test(conn, frm: int, to: int) -> pandas.DataFrame:
#     cur = conn.execute(f"SELECT * from test_data;")
#     return pandas.DataFrame(cur.fetchall())


def get_data_from_source(conn, frm: int, to: int) -> pandas.DataFrame:
    cur = conn.execute(f"SELECT * from {TABLE_NAME} WHERE Instance_ID BETWEEN {frm} AND {to};")
    return pandas.DataFrame(cur.fetchall())


def save_to_destination(conn, df: pandas.DataFrame):
    print("Saving...")
    df.to_sql(TABLE_NAME, con=conn, if_exists="append", index=False)


def main():
    print("Connecting to Redshift...", end=" ")
    source_conn = connect_to_source_db()
    print("Done.")
    print("Connecting to MySQL on RDS...", end=" ")
    dest_conn = connect_to_destination_db()
    print("Done.")
    start, end = 3291846, 3291850
    while True:
        time_start = time()
        df = get_data_from_source(source_conn.cursor(as_dict=True), start, end)
        print(f"Got data. Size: {len(df)}")
        save_to_destination(dest_conn, df)
        print(f"Done. Row Count: {start}")
        print(f"Time took: {time() - time_start}")
        # Change the value to break
        # if row_count >= 100:
        #     break
        start = end + 1
        end += 10000


if __name__ == "__main__":
    main()
