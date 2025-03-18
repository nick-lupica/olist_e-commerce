import src.customers as customers
import src.products as products
import src.orders as orders
import src.common as common

if __name__ == "__main__":
    risposta = "-1"
    while risposta != "0":
        risposta = input("""Che cosa vuoi fare?
        1. Esegui ETL dei customers
        2. Esegui integrazione dei dati regione e città
        3. Format regione per PowerBi
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

    #products.extract()
    #products.transform()
    #products.load()
