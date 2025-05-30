import datetime
import psycopg
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

def read_file():
    is_valid = False
    df = pd.DataFrame()
    while not is_valid:
        path = input("Inserire il path del file:\n").strip()
        try:
            path_list = path.split(".")

            if path_list[-1] == "csv" or path_list[-1] == "txt":
                df = pd.read_csv(path)
            elif path_list[-1] == "xlsx" or path_list[-1] == "xls" :
                df = pd.read_excel(path)
            else:
                df = pd.read_json(path)

        except FileNotFoundError as ex:
            print(ex)
        except OSError as ex:
            print(ex)
        else:
            print("Path inserito correttamente")
            is_valid = True
    else:
        return df

def loading_bar(df,cur,sql):
    print(f"Caricamento in corso... \n{str(len(df))} righe da inserire.")
    Tmax = 50
    if len(df)/2 < 50:
        Tmax = len(df)
    print("┌" + "─" * Tmax + "┐")
    print("│",end="")
    perc_int = 2
    for index, row in df.iterrows():
        perc = float("%.2f" % ((index + 1) / len(df) * 100))
        if perc >= perc_int:
            print("\r│" + "█" * (perc_int//2) + str(int(perc)) + "%",end="")
            #print(perc,end="")
            perc_int += 2
        cur.execute(sql, row.to_list())
    print("\r│" + "█" * Tmax + "│ 100% Completato!")
    print("└" + "─" * Tmax + "┘")

def format_string(df, cols):
    for col in cols:
        df[col] = df[col].str.strip()
        df[col] = df[col].str.replace("[0-9]", "", regex=True)
        df[col] = df[col].str.replace("[\\[\\]$&+:;=?@#|<>.^*(/_)%!]", "", regex=True)
        df[col] = df[col].str.replace(r"\s+", " ", regex = True)
    return df

def format_cap(df):
    df["cap"] = df["cap"].apply(lambda cap: str(int(cap)).zfill(5) if cap == cap else cap)
    return df

def drop_duplicates(df):
    print("Valori duplicati rimossi:\n", df.duplicated().sum(), "\n")
    df.drop_duplicates(inplace = True)
    return df

def check_nulls(df, subset = ""):
    print(f"Valori nulli per colonna:\n {df.isnull().sum()} \n")
    subset = df.columns.tolist()[0] if not subset else subset
    df.dropna(subset=subset, inplace=True, ignore_index=True)
    return df

def fill_nulls(df):
    df.fillna(value="nd", axis=0, inplace=True)
    return df

def format_region():
    print("Formattazione dei nomi delle regioni per powerBi")
    nome_tabella = input("Inserisci nome della tabella da modificare: ").strip().lower()
    with psycopg.connect(host = host,
                         dbname = dbname,
                         user = user,
                         password = password,
                         port = port) as conn:

        with conn.cursor() as cur:
            sql = f"""
                UPDATE {nome_tabella}
                SET region = 'Emilia-Romagna'
                WHERE region = 'Emilia Romagna'
                RETURNING *            
                """

            cur.execute(sql)
            print("record con regione aggiornata")
            for record in cur:
                print(record)

            sql = f"""
                UPDATE {nome_tabella}
                SET region = 'Friuli-Venezia Giulia'
                WHERE region = 'Friuli Venezia Giulia'
                RETURNING *            
                """

            cur.execute(sql)
            print("record con regione aggiornata")
            for record in cur:
                print(record)

            sql = f"""
                UPDATE {nome_tabella}
                SET region = 'Trentino-Alto Adige'
                WHERE region = 'Trentino Alto Adige'
                RETURNING *            
                """

            cur.execute(sql)
            print("record con regione aggiornata")
            for record in cur:
                print(record)


def save_processed(df):
    name = input("What is the name of the file? ").strip().lower()
    timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S")
    file_name = name + "_processedDateTime_" + timestamp + ".csv"
    print(file_name, end="\n\n")
    if __name__ == "__main__":
        directory_name = "../data/processed/"
    else:
        directory_name = "data/processed/"
    df.to_csv(directory_name + file_name, index=False)



if __name__ == "__main__":
    #df = readfile()
    #df = format_string(df, ["region", "city"])
    #print("Dati prima di format cap")
    #print(df.dtypes)
    #print(df)
    #df = format_cap(df)
    #print("Dati dopo format cap")
    #print(df.dtypes)
    #print(df)
    #check_nulls(df)
    #save_processed(df)
    format_region()
