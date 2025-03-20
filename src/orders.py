# ETL methods for ORDERS
import pandas as pd
import src.common as common
import psycopg
from dotenv import load_dotenv
import os
import datetime

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

def extract():
    print("Questo è il metodo EXTRACT degli ordini")
    df = common.read_file()
    return df

def transform(df):
    print("Questo è il metodo TRANSFORM degli ordini")
    df = common.drop_duplicates(df)
    df = common.check_nulls(df, ["order_id", "customer_id"])
    return df


def load(df):
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
    df["order_delivered_customer_date"] = pd.to_datetime(df["order_delivered_customer_date"])
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:

            sql = """
                CREATE TABLE orders (
                pk_order VARCHAR PRIMARY KEY,
                fk_customer VARCHAR,
                status VARCHAR,
                purchase_timestamp TIMESTAMP,
                delivered_timestamp TIMESTAMP,
                estimated_date DATE,
                last_updated TIMESTAMP
                );
                """

            try:
                cur.execute(sql)
            except psycopg.errors.DuplicateTable as ex:
                conn.commit()
                print(ex)
                domanda = input("Vuoi cancellare la tabella? Si/No\n ").strip().upper()
                if domanda == "SI":
                    # eliminare tabella
                    sql_delete = """
                    DROP TABLE orders
                    """
                    cur.execute(sql_delete)
                    print("Tabella orders eliminata.")
                    conn.commit()
                    print("Ricreo la tabella orders.")
                    cur.execute(sql)

            sql = """
            INSERT INTO orders
            (pk_order, fk_customer, status, purchase_timestamp, delivered_timestamp, estimated_date, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (pk_order) DO UPDATE SET 
            (fk_customer, status, purchase_timestamp, delivered_timestamp, estimated_date, last_updated) = (EXCLUDED.fk_customer, EXCLUDED.status, EXCLUDED.purchase_timestamp, EXCLUDED.delivered_timestamp, EXCLUDED.estimated_date, EXCLUDED.last_updated)
            """

            common.caricamento_barra(df, cur, sql)
            conn.commit()

def main():
    print("Questo è il metodo MAIN degli ordini")
    df = extract()
    df = transform(df)
    load(df)

if __name__ == "__main__": # Indica ciò che viene eseguito quando eseguo direttamente
    main()