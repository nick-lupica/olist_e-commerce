import src.common as common
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
    df = common.readfile()
    return df

def transform(df):
    print("Questo è il metodo transform dei clienti")
    df = common.drop_duplicates(df)
    df = common.check_nulls(df)
    df = common.format_cap(df)
    common.save_processed(df)
    print(df)
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
                domanda = input("Vuoi cancellare la tabella? Si/No\n ").strip().upper()
                if domanda == "SI":
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

            common.caricamento_barra(df, cur, sql)

            conn.commit()


def main():
    print("Questo è il metodo main dei clienti")
    df = extract()
    df = transform(df)
    load(df)



if __name__ == "__main__":
   main()