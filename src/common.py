import datetime
import os

import pandas as pd

def readfile():
    is_valid = False
    df = pd.DataFrame()
    while not is_valid:
        path = input("Inserire il path del file:\n").strip()
        try:
            df = pd.read_csv(path)
        except FileNotFoundError as ex:
            print(ex)
        except OSError as ex:
            print(ex)
        else:
            print("Path inserito correttamente")
            is_valid = True
    else:
        return df

def caricamento_barra(df,cur,sql):
    print(f"Caricamento in corso... \n{str(len(df))} righe da inserire.")
    print("┌──────────────────────────────────────────────────┐")
    print("│",end="")
    perc_int = 2
    for index, row in df.iterrows():
        perc = float("%.2f" % ((index + 1) / len(df) * 100))
        if perc >= perc_int:
            print("█",end="")
            #print(perc,end="")
            perc_int += 2
        cur.execute(sql, row.to_list())
    print("│ 100% Completato!")
    print("└──────────────────────────────────────────────────┘")
def format_string(df, cols):
    print(df[cols])
    for col in cols:
        df[col] = df[col].str.strip("./_ ")
        df[col] = df[col].str.replace("[0-9]", "", regex=True)
        df[col] = df[col].str.replace("[$&+:;=?@#|<>.^*()%!]", "", regex=True)
       # df[col] = df[col].str.replace("[ ]", "", regex=True)
    return df

def format_cap(df):
    df["cap"] = df["cap"].astype(str).str.zfill(5)
    return df

def drop_duplicates(df):
    print("Valori duplicati rimossi:\n", df.duplicated().sum(), "\n")
    df.drop_duplicates(inplace = True)
    return df

def check_nulls(df, subset = ""):
    print(f"Valori nulli per colonna:\n {df.isnull().sum()} \n")
    subset = df.columns.tolist()[0] if not subset else subset
    df.dropna(subset=subset, inplace=True, ignore_index=True)
    #df = fill_nulls(df)
    print(df)
    return df

def fill_nulls(df):
    df.fillna(value="nd", axis=0, inplace=True)
    return df

def save_processed(df):
    name = input("Inserisci nome del file ").strip().lower()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = name + "_processed" + "_datetime" + timestamp + ".csv"
    print(file_name)
    if __name__ == "__main__":
        directory_name = "../data/processed/"
    else:
        directory_name = "data/processed/"
    df.to_csv(directory_name + file_name, index=False)



if __name__ == "__main__":
    df = readfile()
    df = format_string(df, ["region", "city"])
    print(df)
    #check_nulls(df)
    #save_processed(df)
