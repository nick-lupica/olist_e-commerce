import src.common as common
import psycopg
import os
from dotenv import load_dotenv
import datetime
import numpy as np
import pandas as pd

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
    # Rinomina le colonne per corrispondere ai nomi desiderati
    df.rename(columns={
        'product_id': 'pk_product',
        'category': 'name_category',
        'product_name_lenght': 'name_length',
        'product_description_lenght': 'description_length',
        'product_photos_qty': 'imgs_qty'
    }, inplace=True)

    # Converti le colonne numeriche in interi
    colonne_da_convertire = ['name_length', 'description_length', 'imgs_qty']
    df[colonne_da_convertire] = df[colonne_da_convertire].apply(lambda x: x.astype(str)) \
        .apply(lambda x: x.str.replace("nan", "0")) \
        .apply(lambda x: x.str.replace(".0", "")) \
        .apply(lambda x: x.astype(int))
    return df


def transform(df):
    print("Questo è il metodo transform di products")
    df = convert(df)
    df = common.drop_duplicates(df)
    return df


def add_fk_column(df):
    df["fk_category"] = None
    df["fk_category"] = np.where(df["name_category"] == "health_beauty", 1,
                                   df["fk_category"])
    df["fk_category"] = np.where(df["name_category"] == "computers_accessories", 2,
                                   df["fk_category"])
    df["fk_category"] = np.where(df["name_category"] == "auto", 3, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "bed_bath_table") |
                                   (df["name_category"] == "housewares") |
                                   (df["name_category"] == "fixed_telephony") |
                                   (df["name_category"] == "home_confort") |
                                   (df["name_category"] == "home_comfort_2") |
                                   (df["name_category"] == "la_cuisine")
                                   , 4, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "furniture_decor") |
                                   (df["name_category"] == "kitchen_dining_laundry_garden_furniture") |
                                   (df["name_category"] == "furniture_mattress_and_upholstery") |
                                   (df["name_category"] == "furniture_living_room") |
                                   (df["name_category"] == "furniture_bedroom")
                                   , 5, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "sports_leisure") |
                                   (df["name_category"] == "fashion_sport")
                                   , 6, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "perfumery")
                                   , 7, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "telephony") |
                                   (df["name_category"] == "")
                                   , 8, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "watches_gifts") |
                                   (df["name_category"] == "fashion_bags_accessories") |
                                   (df["name_category"] == "fashion_shoes") |
                                   (df["name_category"] == "luggage_accessories")
                                   , 9, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "food_drink") |
                                   (df["name_category"] == "food") |
                                   (df["name_category"] == "drinks")
                                   , 10, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "baby") |
                                   (df["name_category"] == "diapers_and_hygiene")
                                   , 11, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "stationery")
                                   , 12, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "tablets_printing_image") |
                                   (df["name_category"] == "office_furniture")
                                   , 13, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "toys")
                                   , 14, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "garden_tools") |
                                   (df["name_category"] == "costruction_tools_garden") |
                                   (df["name_category"] == "construction_tools_construction") |
                                   (df["name_category"] == "costruction_tools_tools") |
                                   (df["name_category"] == "construction_tools_safety") |
                                   (df["name_category"] == "construction_tools_lights") |
                                   (df["name_category"] == "home_construction") |
                                   (df["name_category"] == "security_and_services") |
                                   (df["name_category"] == "signaling_and_security") |
                                   (df["name_category"] == "flowers")
                                   , 15, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "small_appliances") |
                                   (df["name_category"] == "small_appliances_home_oven_and_coffee")
                                   , 16, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "fashion_male_clothing") |
                                   (df["name_category"] == "fashio_female_clothing") |
                                   (df["name_category"] == "fashion_underwear_beach") |
                                   (df["name_category"] == "fashion_childrens_clothes")
                                   , 17, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "consoles_games") |
                                 (df["name_category"] == "pc_gamer")
                                   , 18, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "audio")
                                   , 19, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "cool_stuff")
                                   , 20, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "air_conditioning") |
                                   (df["name_category"] == "home_appliances") |
                                   (df["name_category"] == "home_appliances_2")
                                   , 21, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "pet_shop")
                                   , 22, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "market_place")
                                   , 23, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "electronics") |
                                   (df["name_category"] == "art") |
                                   (df["name_category"] == "arts_and_craftmanship")
                                   , 24, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "party_supplies") |
                                   (df["name_category"] == "christmas_supplies")
                                   , 25, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "agro_industry_and_commerce") |
                                   (df["name_category"] == "industry_commerce_and_business")
                                   , 26, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "books_technical") |
                                   (df["name_category"] == "books_imported") |
                                   (df["name_category"] == "books_general_interest")
                                   , 27, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "musical_instruments") |
                                   (df["name_category"] == "music") |
                                   (df["name_category"] == "cds_dvds_musicals")
                                   , 28, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "computers")
                                   , 29, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "dvds_blu_ray")
                                   , 30, df["fk_category"])

    df["fk_category"] = np.where((df["name_category"] == "cine_photo")
                                   , 31, df["fk_category"])

    df["fk_category"] = np.where(pd.isna(df["name_category"]), 32, df["fk_category"])
    df = df[
        ["pk_product", "name_category", "fk_category", "name_length", "description_length", "imgs_qty"]]

    return df


def caricamento_barra_products(df, cur, sql):
    print(f"Caricamento in corso... \n{len(df)} righe da inserire.")
    Tmax = 50
    if len(df) / 2 < 50:
        Tmax = len(df)
    print("┌" + "─" * Tmax + "┐")
    print("│", end="")
    perc_int = 2
    # Sostituisci i NaN con None
    df = df.where(pd.notna(df), None)

    for index, row in df.iterrows():
        perc = (index + 1) / len(df) * 100
        if perc >= perc_int:
            print("\r│" + "█" * (perc_int // 2) + f"{int(perc)}%", end="")
            perc_int += 2

        # Converte il datetime in stringa e ordina i campi correttamente
        row_data = row[["pk_product", "name_category", "fk_category", "name_length", "description_length", "imgs_qty", "last_updated"]].to_list()
        cur.execute(sql, row_data)

    print("\r│" + "█" * Tmax + "│ 100% Completato!")
    print("└" + "─" * Tmax + "┘")



def load(df):
    # Aggiungi la colonna last_updated
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")

    print("Questo è il metodo load dei clienti")
    with psycopg.connect(host=host,
                         dbname=dbname,
                         user=user,
                         password=password,
                         port=port) as conn:
        with conn.cursor() as cur:
            # Creazione della tabella nel database
            sql_create = """
               CREATE TABLE products (
                   pk_product VARCHAR PRIMARY KEY,
                   name_category VARCHAR,
                   fk_category INTEGER,
                   name_length INTEGER,
                   description_length INTEGER,
                   imgs_qty INTEGER,
                   last_updated TIMESTAMP
               );
               """
            try:
                cur.execute(sql_create)
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
                    cur.execute(sql_create)

            # Inserimento dei dati nel database
            sql_insert = """
               INSERT INTO products
               (pk_product, name_category, fk_category, name_length, description_length, imgs_qty, last_updated)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (pk_product) DO UPDATE
               SET name_category = EXCLUDED.name_category,
                   fk_category = EXCLUDED.fk_category,
                   name_length = EXCLUDED.name_length,
                   description_length = EXCLUDED.description_length,
                   imgs_qty = EXCLUDED.imgs_qty,
                   last_updated = EXCLUDED.last_updated
                   ;
               """

            """for index, row in df.iterrows():
                cur.execute(sql_insert, (
                    row["pk_product"],
                    row["name_category"],
                    row["fk_category"],
                    row["name_length"],
                    row["description_length"],
                    row["imgs_qty"],
                    row["last_updated"]
                ))"""

            caricamento_barra_products(df, cur, sql_insert)

            conn.commit()



def main():
    print("Questo è il metodo main dei products")
    df = extract()
    print("dati traformati")
    print(df)
    df = transform(df)
    df = add_fk_column(df)
    load(df)


if __name__ == "__main__":
    main()
