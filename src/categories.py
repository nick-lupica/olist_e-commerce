import datetime
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
    df = common.read_file()
    return df


def transform(df, column):
    df["category_name"] = None
    df["category_name"] = np.where(df[f"{column}"] == "health_beauty", "beauty",
                                   df["category_name"])
    df["category_name"] = np.where(df[f"{column}"] == "computers_accessories", "it accessories",
                                   df["category_name"])
    df["category_name"] = np.where(df[f"{column}"] == "auto", "automotive", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "bed_bath_table") |
                                   (df[f"{column}"] == "housewares") |
                                   (df[f"{column}"] == "fixed_telephony") |
                                   (df[f"{column}"] == "home_confort") |
                                   (df[f"{column}"] == "home_comfort_2") |
                                   (df[f"{column}"] == "la_cuisine")
                                   , "home & kitchen", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "furniture_decor") |
                                   (df[f"{column}"] == "kitchen_dining_laundry_garden_furniture") |
                                   (df[f"{column}"] == "furniture_mattress_and_upholstery") |
                                   (df[f"{column}"] == "furniture_living_room") |
                                   (df[f"{column}"] == "furniture_bedroom")
                                   , "furniture & decor", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "sports_leisure") |
                                   (df[f"{column}"] == "fashion_sport")
                                   , "sport", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "perfumery")
                                   , "perfumes", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "telephony") |
                                   (df[f"{column}"] == "")
                                   , "smartphones", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "watches_gifts") |
                                   (df[f"{column}"] == "fashion_bags_accessories") |
                                   (df[f"{column}"] == "fashion_shoes") |
                                   (df[f"{column}"] == "luggage_accessories")
                                   , "accessories", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "food_drink") |
                                   (df[f"{column}"] == "food") |
                                   (df[f"{column}"] == "drinks")
                                   , "food", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "baby") |
                                   (df[f"{column}"] == "diapers_and_hygiene")
                                   , "baby", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "stationery")
                                   , "stationery", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "tablets_printing_image") |
                                   (df[f"{column}"] == "office_furniture")
                                   , "office", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "toys")
                                   , "toys", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "garden_tools") |
                                   (df[f"{column}"] == "costruction_tools_garden") |
                                   (df[f"{column}"] == "construction_tools_construction") |
                                   (df[f"{column}"] == "costruction_tools_tools") |
                                   (df[f"{column}"] == "construction_tools_safety") |
                                   (df[f"{column}"] == "construction_tools_lights") |
                                   (df[f"{column}"] == "home_construction") |
                                   (df[f"{column}"] == "security_and_services") |
                                   (df[f"{column}"] == "signaling_and_security") |
                                   (df[f"{column}"] == "flowers")
                                   , "home improvement & garden", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "small_appliances") |
                                   (df[f"{column}"] == "small_appliances_home_oven_and_coffee")
                                   , "small appliances", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "fashion_male_clothing") |
                                   (df[f"{column}"] == "fashio_female_clothing") |
                                   (df[f"{column}"] == "fashion_underwear_beach") |
                                   (df[f"{column}"] == "fashion_childrens_clothes")
                                   , "fashion", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "consoles_games") |
                                   (df[f"{column}"] == "pc_gamer")
                                   , "video games & consoles", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "audio")
                                   , "audio", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "cool_stuff")
                                   , "gift ideas", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "air_conditioning") |
                                   (df[f"{column}"] == "home_appliances") |
                                   (df[f"{column}"] == "home_appliances_2")
                                   , "home appliances", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "pet_shop")
                                   , "pet", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "market_place")
                                   , "used", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "electronics") |
                                   (df[f"{column}"] == "art") |
                                   (df[f"{column}"] == "arts_and_craftmanship")
                                   , "bricolage", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "party_supplies") |
                                   (df[f"{column}"] == "christmas_supplies")
                                   , "seasonal", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "agro_industry_and_commerce") |
                                   (df[f"{column}"] == "industry_commerce_and_business")
                                   , "commerce", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "books_technical") |
                                   (df[f"{column}"] == "books_imported") |
                                   (df[f"{column}"] == "books_general_interest")
                                   , "books", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "musical_instruments") |
                                   (df[f"{column}"] == "music") |
                                   (df[f"{column}"] == "cds_dvds_musicals")
                                   , "music", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "computers")
                                   , "computer", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "dvds_blu_ray")
                                   , "dvd blu-ray", df["category_name"])

    df["category_name"] = np.where((df[f"{column}"] == "cine_photo")
                                   , "photography & video", df["category_name"])

    df["category_name"] = df["category_name"].fillna("other")  # Imposta 'other' per i valori non mappati
    df["category_name"] = np.where((df[f"{column}"] == ""), "other",
                                   df["category_name"])



    return df


def load_categories(df):
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    print("Questo è il metodo load delle categorie")
    with psycopg.connect(host=host,
                         dbname=dbname,
                         user=user,
                         password=password,
                         port=port) as conn:

        with conn.cursor() as cur:

            sql = """
            CREATE TABLE categories(
            pk_category SERIAL PRIMARY KEY,
            category_name VARCHAR UNIQUE,
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
                    # eliminare tabella
                    sql_delete = """
                    DROP TABLE categories
                    """
                    cur.execute(sql_delete)
                    print("Tabella categories eliminata.")
                    conn.commit()
                    print("Ricreo la tabella categories.")
                    cur.execute(sql)

            sql = """INSERT INTO categories 
            (category_name, last_updated) 
            VALUES (%s, %s) ON CONFLICT (category_name) DO UPDATE SET
            last_updated = EXCLUDED.last_updated;
            """

            print(f"Caricamento in corso... {str(len(df))} righe da inserire.")
            print("Rimossi valori di category doppioni\n")

            common.caricamento_barra(df, cur, sql)

            conn.commit()

def load(df):
    categories_list = df["category_name"].unique()
    categories_list = pd.DataFrame(categories_list)
    print(categories_list)
    common.drop_duplicates(df["category_name"])
    load_categories(categories_list)

def main():
    print("Questo è il metodo main delle categorie")
    df = extract()
    df = transform(df, "product_category_name_english")
    load(df)


if __name__ == "__main__":
    main()