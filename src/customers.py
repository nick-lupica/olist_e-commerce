from src.common import readfile, caricamento_barra
import psycopg
import os
from dotenv import load_dotenv

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

def extract():
    print("Questo è il metodo extract dei clienti")
    df = readfile()
    return df

def transform(df):
    print("Questo è il metodo transform dei clienti")
    return df

def load(df):
    print("Questo è il metodo load dei clienti")
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            # cur execute

            sql = """
            CREATE TABLE customers (
            pk_customer VARCHAR PRIMARY KEY,
            region VARCHAR,
            city VARCHAR,
            cap VARCHAR
            );
            """

            try:
                cur.execute(sql)
            except psycopg.errors.DuplicateTable as ex:
                conn.commit()
                print(ex)
                domanda = input("Vuoi cancellare la tabella? Si/No\n ").strip()
                if domanda == "Si":
                    #eliminare tabella
                    sql_delete = """
                    DROP TABLE customers
                    """
                    cur.execute(sql_delete)
                    print("Tabella customers eliminata.")
                    conn.commit()
                    print("Ricreo la tabella customers.")
                    cur.execute(sql)



            # Inserimento report nel database

            sql = """
            INSERT INTO customers
            (pk_customer, region, city, cap)
            VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;
            """

            caricamento_barra(df, cur, sql)

            conn.commit()


def main():
    print("Questo è il metodo main dei clienti")
    df = extract()
    df = transform(df)
    load(df)



if __name__ == "__main__":
   main()