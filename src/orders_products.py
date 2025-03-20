# ETL methods for ORDERS_PRODUCTS

import src.common as common
#from src.common import read_file, caricamento_barra
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
    print("EXTRACT oder_products")
    df = common.read_file()
    return df

def transform(df):
    print("TRANSFORM order_products")
    df = common.drop_duplicates(df)
    df = common.check_nulls(df, ["order_id", "product_id", "seller_id"])
    return df


def load(df):
    print("LOAD order_products")
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")

    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:

            sql = """
                CREATE TABLE IF NOT EXISTS orders_products (
                pk_order_product SERIAL PRIMARY KEY,
                fk_order VARCHAR ,
                order_item INTEGER,
                fk_product VARCHAR,
                fk_seller VARCHAR,
                price FLOAT,
                freight  FLOAT,
                last_updated TIMESTAMP,
                FOREIGN KEY(fk_order) REFERENCES orders (pk_order),
                FOREIGN KEY(fk_product) REFERENCES products (pk_product)
                );
                """
            cur.execute(sql)

            sql = """
            INSERT INTO orders_products
            (fk_order, order_item, fk_product, fk_seller, price, freight, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (pk_order_product) DO UPDATE SET 
            (fk_order,order_item, fk_product, fk_seller, price, freight, last_updated) = 
            (EXCLUDED.fk_order, EXCLUDED.order_item, EXCLUDED.fk_product,
            EXCLUDED.fk_seller,EXCLUDED.price,EXCLUDED.freight, EXCLUDED.last_updated);
            """

            common.caricamento_barra(df, cur, sql)
            conn.commit()

def main():
    print("Questo è il metodo MAIN degli ordini")
    df = extract()
    df = transform(df)
    load(df)




if __name__ == "__main__":
   main()