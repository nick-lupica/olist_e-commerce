# ETL methods for ORDERS_PRODUCTS

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
    print("EXTRACT oder_products")
    df = common.read_file()
    return df

def transform(df):
    print("TRANSFORM order_products")
    df = common.drop_duplicates(df)
    df = common.check_nulls(df, ["order_id", "product_id", "seller_id"])
    return df


def load(df):
    print("LOAD orders_products")
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
            try:
                cur.execute(sql)
            except psycopg.errors.DuplicateTable as ex:
                conn.commit()
                print(ex)
                domanda = input("Vuoi cancellare la tabella? Si/No\n ").strip().upper()
                if domanda == "SI":
                    #eliminare tabella
                    sql_delete = """
                    DROP TABLE orders_products CASCADE
                    """
                    cur.execute(sql_delete)
                    print("Tabella orders_products eliminata.")
                    conn.commit()
                    print("Ricreo la tabella orders_products.")
                    cur.execute(sql)

            sql = """
            INSERT INTO orders_products
            (fk_order, order_item, fk_product, fk_seller, price, freight, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (pk_order_product) DO UPDATE SET 
            (fk_order,order_item, fk_product, fk_seller, price, freight, last_updated) = 
            (EXCLUDED.fk_order, EXCLUDED.order_item, EXCLUDED.fk_product,
            EXCLUDED.fk_seller,EXCLUDED.price,EXCLUDED.freight, EXCLUDED.last_updated);
            """

            common.loading_bar(df, cur, sql)
            conn.commit()

def delete_invalid_order():
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """
            DELETE
            FROM orders_products
            WHERE fk_order IN (SELECT pk_order FROM orders
            WHERE orders.delivered_timestamp IS NULL AND orders.status = 'delivered')
            """
            cur.execute(sql)
            sql = """
            DELETE 
            FROM orders
            WHERE orders.delivered_timestamp IS NULL AND orders.status = 'delivered'
            """
            cur.execute(sql)
            conn.commit()

def main():
    print("Questo è il metodo MAIN degli ordini")
    df = extract()
    df = transform(df)
    load(df)





if __name__ == "__main__":
   main()