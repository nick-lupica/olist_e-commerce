from src.common import readfile
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
    print(df)
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            # cur execute

            sql = """
            CREATE TABLE IF NOT EXISTS customers (
            pk_customer VARCHAR PRIMARY KEY,
            region VARCHAR,
            city VARCHAR,
            cap VARCHAR
            );
            """

            cur.execute(sql)

            # Inserimento report nel database

            sql = """
            INSERT INTO customers
            (pk_customer, region, city, cap)
            VALUES (%s, %s, %s, %s);
            """

            for indx, row in df.iterrows():
                cur.execute(sql, row.to_list())

            conn.commit()


def main():
    print("Questo è il metodo main dei clienti")
    df = extract()
    df = transform(df)
    load(df)



if __name__ == "__main__":
   main()