import src.customers as customers
import src.categories as categories
import src.products as products
import src.orders as orders
import src.common as common

if __name__ == "__main__":
    risposta = "-1"
    while risposta != "0":
        domanda_iniziale = input("""Su quale tabella vuoi svolgere delle operazioni?
        1. customers
        2. categories
        3. products
        4. orders
        5. orders_products
        0. esci dal programma
        """)
        if domanda_iniziale == "1":
            risposta = input("""Che cosa vuoi fare con la tabella customers?
            1. Esegui ETL dei customers
            2. Esegui integrazione dei dati regione e città
            3. Format nomi regione per PowerBi
            0. Esci dal programma
            """)
            if risposta == "1":
                df_costumers = customers.extract()
                df_costumers = customers.transform(df_costumers)
                customers.load(df_costumers)
            elif risposta == "2":
                customers.complete_city_region()
            elif risposta == "3":
                common.format_region()
            else:
                risposta = "0"

        if domanda_iniziale == "2":
            risposta = input("""Che cosa vuoi fare con la tabella categories?
            1. Esegui ETL dei categories
            0. Esci dal programma
            """)
            if risposta == "1":
                df_categories = categories.extract()
                df_categories = categories.load_new_column(df_categories)
                categories.load(df_categories)
            else:
                risposta = "0"




    #products.extract()
    #products.transform()
    #products.load()
