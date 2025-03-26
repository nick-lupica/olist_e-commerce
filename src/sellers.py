import src.common as common
import psycopg
import os
from dotenv import load_dotenv
import datetime

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

def extract():
    print("Questo è il metodo extract dei sellers")
    df = common.read_file()
    return df

def transform(df):
    print("Questo è il metodo transform dei sellers")
    df = common.drop_duplicates(df)
    df = common.check_nulls(df, ["seller_id"])
    df = common.format_string(df, ["region"])
    common.save_processed(df)
    return df

def load(df):
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    print("Questo è il metodo load dei sellers")
    with psycopg.connect(host=host,
                         dbname=dbname,
                         user=user,
                         password=password,
                         port=port) as conn:
        with conn.cursor() as cur:
            # cur execute

            sql = """
                CREATE TABLE sellers (
                pk_seller VARCHAR PRIMARY KEY,
                region VARCHAR,
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
                    #eliminare tabella
                    sql_delete = """
                        DROP TABLE sellers CASCADE
                        """
                    cur.execute(sql_delete)
                    print("Tabella sellers eliminata.")
                    conn.commit()
                    print("Ricreo la tabella sellers.")
                    cur.execute(sql)

            # Inserimento report nel database

            sql = """
                INSERT INTO sellers
                (pk_seller, region, last_updated)
                VALUES (%s, %s, %s) ON CONFLICT (pk_seller) DO UPDATE SET
                (region, last_updated) = (EXCLUDED.region, EXCLUDED.last_updated);
                """

            common.loading_bar(df, cur, sql)

            conn.commit()


def main():
    print("Questo è il metodo main dei sellers")
    df = extract()
    df = transform(df)
    print("dati traformati")
    print(df)
    load(df)



if __name__ == "__main__":
   main()