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


def load_new_column(df):
    df["category_name"] = None
    df["category_name"] = np.where(df["product_category_name_english"] == "health_beauty", "beauty",
                                   df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "computers_accessories", "it accessories",
                                   df["category_name"])
    df["category_name"] = np.where(df["product_category_name_english"] == "auto", "automotive", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "bed_bath_table") |
                                   (df["product_category_name_english"] == "housewares") |
                                   (df["product_category_name_english"] == "fixed_telephony") |
                                   (df["product_category_name_english"] == "home_confort") |
                                   (df["product_category_name_english"] == "home_comfort_2") |
                                   (df["product_category_name_english"] == "la_cuisine")
                                   , "home & kitchen", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "furniture_decor") |
                                   (df["product_category_name_english"] == "kitchen_dining_laundry_garden_furniture") |
                                   (df["product_category_name_english"] == "furniture_mattress_and_upholstery") |
                                   (df["product_category_name_english"] == "furniture_living_room") |
                                   (df["product_category_name_english"] == "furniture_bedroom")
                                   , "furniture & decor", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "sports_leisure") |
                                   (df["product_category_name_english"] == "fashion_sport")
                                   , "sport", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "perfumery")
                                   , "perfumes", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "telephony") |
                                   (df["product_category_name_english"] == "")
                                   , "smartphones", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "watches_gifts") |
                                   (df["product_category_name_english"] == "fashion_bags_accessories") |
                                   (df["product_category_name_english"] == "fashion_shoes") |
                                   (df["product_category_name_english"] == "luggage_accessories")
                                   , "accessories", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "food_drink") |
                                   (df["product_category_name_english"] == "food") |
                                   (df["product_category_name_english"] == "drinks")
                                   , "food", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "baby") |
                                   (df["product_category_name_english"] == "diapers_and_hygiene")
                                   , "baby", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "stationery")
                                   , "stationery", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "tablets_printing_image") |
                                   (df["product_category_name_english"] == "office_furniture")
                                   , "office", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "toys")
                                   , "toys", df["category_name"])

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
                                   , "home improvement & garden", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "small_appliances") |
                                   (df["product_category_name_english"] == "small_appliances_home_oven_and_coffee")
                                   , "small appliances", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "fashion_male_clothing") |
                                   (df["product_category_name_english"] == "fashio_female_clothing") |
                                   (df["product_category_name_english"] == "fashion_underwear_beach") |
                                   (df["product_category_name_english"] == "fashion_childrens_clothes")
                                   , "fashion", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "consoles_games") |
                                   (df["product_category_name_english"] == "pc_gamer")
                                   , "video games & consoles", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "audio")
                                   , "audio", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "cool_stuff")
                                   , "gift ideas", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "air_conditioning") |
                                   (df["product_category_name_english"] == "home_appliances") |
                                   (df["product_category_name_english"] == "home_appliances_2")
                                   , "home appliances", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "pet_shop")
                                   , "pet", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "market_place")
                                   , "used", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "electronics") |
                                   (df["product_category_name_english"] == "art") |
                                   (df["product_category_name_english"] == "arts_and_craftmanship")
                                   , "bricolage", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "party_supplies") |
                                   (df["product_category_name_english"] == "christmas_supplies")
                                   , "seasonal", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "agro_industry_and_commerce") |
                                   (df["product_category_name_english"] == "industry_commerce_and_business")
                                   , "commerce", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "books_technical") |
                                   (df["product_category_name_english"] == "books_imported") |
                                   (df["product_category_name_english"] == "books_general_interest")
                                   , "books", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "musical_instruments") |
                                   (df["product_category_name_english"] == "music") |
                                   (df["product_category_name_english"] == "cds_dvds_musicals")
                                   , "music", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "computers")
                                   , "computer", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "dvds_blu_ray")
                                   , "dvd blu-ray", df["category_name"])

    df["category_name"] = np.where((df["product_category_name_english"] == "cine_photo")
                                   , "photography & video", df["category_name"])


    df["category_name"] = df["category_name"].fillna("other")  # Imposta 'other' per i valori non mappati

    # **Aggiunge "other" manualmente come categoria se non esiste già**
    if "other" not in df["category_name"].unique():
        df = pd.concat([df, pd.DataFrame([{"category_name": "other"}])], ignore_index=True)

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
            CREATE TABLE categories(
            pk_category SERIAL PRIMARY KEY,
            category VARCHAR UNIQUE
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
            (category) 
            VALUES (%s) ON CONFLICT (category) DO NOTHING;
            """

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
    print("dati traformati")
    print(df)
    df = load_new_column(df)
    load(df)





if __name__ == "__main__":
   main()