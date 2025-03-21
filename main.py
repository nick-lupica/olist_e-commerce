import src.customers as customers
import src.categories as categories
import src.products as products
import src.orders as orders
import src.common as common
import src.orders_products as orders_products

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
                df_categories = categories.transform(df_categories, "product_category_name_english")
                categories.load(df_categories)
            else:
                risposta = "0"

        if domanda_iniziale == "3":
            risposta = input("""Che cosa vuoi fare con la tabella products?
            1. Esegui ETL dei products
            0. Esci dal programma
            """)
            if risposta == "1":
                df_products = products.extract()
                df_products = products.transform(df_products)
                df_products = products.raw_load(df_products)

            else:
                risposta = "0"

        if domanda_iniziale == "4":
            risposta = input("""Che cosa vuoi fare con la tabella orders?
            1. Esegui ETL di orders
            0. Esci dal programma
            """)
            if risposta == "1":
                df_orders = orders.extract()
                df_orders = orders.transform(df_orders)
                orders.load(df_orders)
            else:
                risposta = "0"

        if domanda_iniziale == "5":
            risposta = input("""Che cosa vuoi fare con la tabella orders_products?
            1. Esegui ETL di orders_products
            2. Elimina valori null con status 'delivered'
            0. Esci dal programma
            """)
            if risposta == "1":
                df_orders_products = orders_products.extract()
                df_orders_products = orders_products.transform(df_orders_products)
                orders_products.load(df_orders_products)
            elif risposta == "2":
                orders_products.delete_invalid_order()
            else:
                risposta = "0"

        if domanda_iniziale == "0":
            risposta = "0"


    #products.extract()
    #products.transform()
    #products.load()
