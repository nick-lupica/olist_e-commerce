import src.common as common
import psycopg
import os
from dotenv import load_dotenv
import numpy as np
import pandas as pd

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

def extract():
    print("Questo è il metodo extract delle categorie")
    df = common.readfile()
    return df

def transform(df):
    print("Questo è il metodo transform delle categorie")
    df = common.drop_duplicates(df)
    df = common.check_nulls(df, ["product_category_name_english", "product_category_name_italian"])

    return df


def load_new_column(df):
    df["category_name"] = None
    df["category_name"] = np.where(df["product_category_name_english"] == "health_beauty", "beauty",
                                   df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "computers_accessories", "informatica",
                                   df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "auto", "automobili", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "bed_bath_table") |
                                   (df["product_category_name_english"] == "housewares") |
                                   (df["product_category_name_english"] == "fixed_telephony") |
                                   (df["product_category_name_english"] == "home_confort") |
                                   (df["product_category_name_english"] == "home_comfort_2") |
                                   (df["product_category_name_english"] == "la_cuisine")
                                   , "casalinghi", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "furniture_decor") |
                                   (df["product_category_name_english"] == "kitchen_dining_laundry_garden_furniture") |
                                   (df["product_category_name_english"] == "furniture_mattress_and_upholstery") |
                                   (df["product_category_name_english"] == "furniture_living_room") |
                                   (df["product_category_name_english"] == "furniture_bedroom")
                                   , "arredamento", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "sports_leisure") |
                                   (df["product_category_name_english"] == "fashion_sport")
                                   , "sport", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "perfumery")
                                   , "profumeria", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "telephony") |
                                   (df["product_category_name_english"] == "")
                                   , "smartphone", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "watches_gifts") |
                                   (df["product_category_name_english"] == "fashion_bags_accessories") |
                                   (df["product_category_name_english"] == "fashion_shoes") |
                                   (df["product_category_name_english"] == "luggage_accessories")
                                   , "accessori", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "food_drink") |
                                   (df["product_category_name_english"] == "food") |
                                   (df["product_category_name_english"] == "drinks")
                                   , "food", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "baby") |
                                   (df["product_category_name_english"] == "diapers_and_hygiene")
                                   , "baby", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "stationery")
                                   , "cartoleria", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "tablets_printing_image") |
                                   (df["product_category_name_english"] == "office_furniture")
                                   , "ufficio", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "toys")
                                   , "giocattoli", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "garden_tools") |
                                   (df["product_category_name_english"] == "costruction_tools_garden") |
                                   (df["product_category_name_english"] == "construction_tools_construction") |
                                   (df["product_category_name_english"] == "costruction_tools_tools") |
                                   (df["product_category_name_english"] == "construction_tools_safety") |
                                   (df["product_category_name_english"] == "construction_tools_lights") |
                                   (df["product_category_name_english"] == "home_construction") |
                                   (df["product_category_name_english"] == "security_and_services") |
                                   (df["product_category_name_english"] == "signaling_and_security") |
                                   (df["product_category_name_english"] == "flowers")
                                   , "edilizia e giardino", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "small_appliances") |
                                   (df["product_category_name_english"] == "small_appliances_home_oven_and_coffee")
                                   , "piccoli elettrodomestici", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "fashion_male_clothing") |
                                   (df["product_category_name_english"] == "fashio_female_clothing") |
                                   (df["product_category_name_english"] == "fashion_underwear_beach") |
                                   (df["product_category_name_english"] == "fashion_childrens_clothes")
                                   , "abbigliamento", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "consoles_games")
                                   , "videogiochi", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "audio")
                                   , "audio", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "cool_stuff")
                                   , "idee regalo", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "air_conditioning") |
                                   (df["product_category_name_english"] == "home_appliances") |
                                   (df["product_category_name_english"] == "home_appliances_2")
                                   , "grandi elettrodomestici", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "pet_shop")
                                   , "animali", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "market_place")
                                   , "usato", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "electronics") |
                                   (df["product_category_name_english"] == "art") |
                                   (df["product_category_name_english"] == "arts_and_craftmanship")
                                   , "bricolage", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "party_supplies") |
                                   (df["product_category_name_english"] == "christmas_supplies")
                                   , "seasonal", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "agro_industry_and_commerce") |
                                   (df["product_category_name_english"] == "industry_commerce_and_business")
                                   , "commercio", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "books_technical") |
                                   (df["product_category_name_english"] == "books_imported") |
                                   (df["product_category_name_english"] == "books_general_interest")
                                   , "libri", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "musical_instruments") |
                                   (df["product_category_name_english"] == "music") |
                                   (df["product_category_name_english"] == "cds_dvds_musicals")
                                   , "musica", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "computers")
                                   , "computer", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "dvds_blu_ray")
                                   , "dvd blu-ray", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "cine_photo")
                                   , "fotografia e video", df["category_name"])
    return df

def load(df):
    print("Questo è il metodo load delle categorie")
    with psycopg.connect(host = host,
                         dbname = dbname,
                         user = user,
                         password = password,
                         port = port) as conn:

        with conn.cursor() as cur:

            sql = """
            CREATE TABLE IF NOT EXISTS categories(
            pk_category SERIAL PRIMARY KEY,
            category VARCHAR
            );
            """

            cur.execute(sql)

            sql = "INSERT INTO categories (category) VALUES (%s);"

            print(f"Caricamento in corso... {str(len(df))} righe da inserire.")
            print("Rimossi valori di category doppioni\n")

            for category in df["category_name"].dropna().unique():
                # DEBUG
                print(category)
                cur.execute(sql, (category,))


            conn.commit()

def main():
    print("Questo è il metodo main delle categorie")
    df = extract()
    df = transform(df)
    print("dati traformati")
    print(df)
    df = load_new_column(df)
    load(df)





if __name__ == "__main__":
   main()