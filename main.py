import src.customers as customers
import src.products as products
import src.orders as orders

if __name__ == "__main__":
    df_costumers = customers.extract()

    df_costumers = customers.transform(df_costumers)

    customers.load(df_costumers)

    products.extract()

    products.transform()

    products.load()
