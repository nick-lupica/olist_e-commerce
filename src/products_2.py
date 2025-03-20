import src.common as common
import psycopg
import os
from dotenv import load_dotenv
import datetime
import numpy as np
import pandas as pd
import src.categories as categories

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")


def extract():
    print("Questo è il metodo extract di products")
    df = common.readfile()
    return df

def convert(df):
    # Converti le colonne numeriche in interi
    colonne_da_convertire = ['name_length', 'description_length', 'imgs_qty']
    df[colonne_da_convertire] = df[colonne_da_convertire].apply(lambda x: x.astype(str)) \
        .apply(lambda x: x.str.replace("nan", "0")) \
        .apply(lambda x: x.str.replace(".0", "")) \
        .apply(lambda x: x.astype(int))
    return df

def load(df):
    categories.load_new_column(df)
    convert(df)
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    df = df[["product_id", "category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty", "last_updated"]]
    with psycopg.connect(host=host,
                         dbname=dbname,
                         user=user,
                         password=password,
                         port=port) as conn:
        with conn.cursor() as cur:
            # Creazione della tabella nel database
            sql = """
               CREATE TABLE products (
                   pk_product VARCHAR PRIMARY KEY,
                   fk_category VARCHAR,
                   name_length INTEGER,
                   description_length INTEGER,
                   imgs_qty INTEGER,
                   last_updated TIMESTAMP
               );
               """
            try:
                cur.execute(sql)
            except psycopg.errors.DuplicateTable as ex:
                conn.commit()
                print(ex)
                domanda = input("Vuoi cancellare la products? Si/No\n ").strip().upper()
                if domanda == "SI":
                    # eliminare tabella
                    sql_delete = """
                                DROP TABLE products
                                """
                    cur.execute(sql_delete)
                    print("Tabella products eliminata.")
                    conn.commit()
                    print("Ricreo la tabella products.")
                    cur.execute(sql)

            sql = """
            INSERT INTO products
            (pk_product, fk_category, name_length, description_length, imgs_qty, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (pk_product) DO UPDATE SET 
            (fk_category, name_length, description_length, imgs_qty, last_updated) = (EXCLUDED.fk_category,
            EXCLUDED.name_length, EXCLUDED.description_length, EXCLUDED.imgs_qty, EXCLUDED.last_updated)
            ;
            """

            common.caricamento_barra(df, cur, sql)
            conn.commit()

def change_category():
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """
            SELECT fk_category
            FROM products ;
            """
            cur.execute(sql)
            print(f"List of records: {cur.rowcount},they are: ")
            for record in cur:
                print(record)

            sql = f"""
            UPDATE products AS  p
            SET fk_category = c.pk_category, 
            last_updated = '{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
            FROM categories AS c 
            WHERE p.fk_category = c.category_name 
            RETURNING *;
            """

            cur.execute(sql)
            updated_records = cur.fetchall()
            for record in updated_records:
                print(record)

            conn.commit()

def main():
    df = extract()
    load(df)
    change_category()

if __name__ == "__main__":
    main()
