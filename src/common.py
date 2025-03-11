import pandas as pd


def readfile():
    isValid = False
    df = pd.DataFrame()
    while not isValid:
        path = input("Inserire il path del file:\n").strip()
        try:
            df = pd.read_csv(path)
        except FileNotFoundError as ex:
            print(ex)
        except OSError as ex:
            print(ex)
        else:
            print("Path inserito correttamente")
            isValid = True
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

if __name__ == "__main__":
    readfile()