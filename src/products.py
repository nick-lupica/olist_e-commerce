# ELT method for products
import pandas as pd
import src.common as common
import psycopg
from dotenv import load_dotenv
import os
import datetime
import src.categories as category

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")


def extract():
    print("---EXTRACT PRODUCTS---")
    df = common.read_file()
    return df


def convert_numbers(df):
    colonne_da_convertire = ['product_name_lenght', 'product_description_lenght', 'product_photos_qty']
    df[colonne_da_convertire] = df[colonne_da_convertire].apply(lambda x: x.astype(str))
    df[colonne_da_convertire] = df[colonne_da_convertire].apply(lambda x: x.str.replace("nan", "0"))
    df[colonne_da_convertire] = df[colonne_da_convertire].apply(lambda x: x.str.replace(".0", ""))
    df[colonne_da_convertire] = df[colonne_da_convertire].apply(lambda x: x.astype(int))
    return df


def raw_load(df):
    print("---LOAD RAW PRODUCTS---")
    category.transform(df, "category")
    convert_numbers(df)
    df = df[["product_id", "category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty"]]
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """
            CREATE TABLE IF NOT EXISTS products (
            pk_product VARCHAR PRIMARY KEY,
            fk_category VARCHAR,
            name_length INTEGER,
            description_length INTEGER,
            imgs_qty INTEGER
            );
            """

            cur.execute(sql)
            print("Sto inserendo le categories come stringhe")
            sql = """
                INSERT INTO products
                (pk_product, fk_category,name_length, description_length, imgs_qty)
                VALUES (%s, %s, %s, %s, %s) ON CONFLICT (pk_product) DO UPDATE SET 
                (fk_category,name_length, description_length, imgs_qty) = (EXCLUDED.fk_category,
                EXCLUDED.name_length,  EXCLUDED.description_length, EXCLUDED.imgs_qty);
                """

            common.loading_bar(df, cur, sql)
            conn.commit()
            change_category()
            null_categories()
            # query useful for creating a new df
            sql = """ SELECT * from products;"""
            cur.execute(sql)
            #rows = cur.fetchall()
            # creo df
            df_update = pd.DataFrame(cur, columns=["pk_product", "fk_category","name_length", "description_length", "imgs_qty"])
            df_update["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")

            sql = """DROP TABLE products CASCADE;"""
            cur.execute(sql)
            conn.commit()

            print("Ricreo tabella products")

            sql = """
                        CREATE TABLE IF NOT EXISTS products (
                        pk_product VARCHAR PRIMARY KEY,
                        fk_category INTEGER,
                        name_length INTEGER,
                        description_length INTEGER,
                        imgs_qty INTEGER,
                        last_updated TIMESTAMP,
                        FOREIGN KEY (fk_category) REFERENCES categories (pk_category)
                        );
                        """

            cur.execute(sql)

            sql = """
                            INSERT INTO products
                            (pk_product, fk_category,name_length, description_length, imgs_qty, last_updated)
                            VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (pk_product) DO UPDATE SET 
                            (fk_category,name_length, description_length, imgs_qty, last_updated) = (EXCLUDED.fk_category,
                            EXCLUDED.name_length,  EXCLUDED.description_length, EXCLUDED.imgs_qty, EXCLUDED.last_updated);
                            """

            common.loading_bar(df_update, cur, sql)


    common.save_processed(df_update)
    return df_update


def change_category():
    print("---CHANGE CATEGORY---")
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = f"""
                          UPDATE products AS  p
                          SET fk_category = c.pk_category
                          FROM categories AS c 
                          WHERE p.fk_category = c.category_name 
                          RETURNING *;
                         """

            cur.execute(sql)
            #updated_records = cur.fetchall()
            for record in cur:
                print(record)
            conn.commit()


def null_categories():
    print("---NULL CATEGORIES---")
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = f"""
                          UPDATE products AS  p
                          SET fk_category = c.pk_category
                          FROM categories AS c 
                          WHERE fk_category IS NULL 
                          RETURNING *;
                         """

            cur.execute(sql)
            #updated_records = cur.fetchall()
            for record in cur:
                print(record)

            conn.commit()


def transform(df):
    print("---TRANSFORM PRODUCTS---")
    df = common.drop_duplicates(df)
    df = common.check_nulls(df, ["product_id"])
    return df


def load(df):
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """
               CREATE TABLE products (
               pk_product VARCHAR PRIMARY KEY,
               fk_category INTEGER,
               name_length INTEGER,
               description_length INTEGER,
               imgs_qty INTEGER,
               last_updated TIMESTAMP,
               FOREIGN KEY (fk_category) REFERENCES categories (pk_category)
               );
               """
            try:
                cur.execute(sql)
            except psycopg.errors.DuplicateTable as ex:
                print(ex)
                conn.commit()
                delete = input("Do you want to delete table? Y/N ").upper().strip()
                if delete == "Y":
                    sql_delete = """ 
                    DROP TABLE products CASCADE;
                    """
                    cur.execute(sql_delete)
                    conn.commit()
                    print("Recreating products table")
                    cur.execute(sql)
            sql = """
                   INSERT INTO products
                   (pk_product, fk_category,name_length, description_length, imgs_qty, last_updated)
                   VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (pk_product) DO UPDATE SET 
                   (fk_category,name_length, description_length, imgs_qty, last_updated) = (EXCLUDED.fk_category,
                   EXCLUDED.name_length, EXCLUDED.description_length, EXCLUDED.imgs_qty, EXCLUDED.last_updated);
                   """

            common.loading_bar(df, cur, sql)
            conn.commit()

def main():
    df = extract()
    raw_load(df)

if __name__ == "__main__":
    main()