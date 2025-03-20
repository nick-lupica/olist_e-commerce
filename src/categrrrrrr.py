def load(df):
    with psycopg.connect(host=host,
                         dbname=dbname,
                         user=user,
                         password=password,
                         port=port) as conn:
        with conn.cursor() as cur:
            # Creazione della tabella nel database
            sql = """
               CREATE TABLE IF NOT EXISTS products (
                   pk_product VARCHAR PRIMARY KEY,
                   fk_category INTEGER,
                   name_length INTEGER,
                   description_length INTEGER,
                   imgs_qty INTEGER
               );
               """
            cur.execute(sql)
            conn.commit()

            sql = """
            INSERT INTO products
            (pk_product, fk_category, name_length, description_length, imgs_qty)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (pk_product) DO UPDATE SET 
            (fk_category, name_length, description_length, imgs_qty) = (EXCLUDED.fk_category,
            EXCLUDED.name_length, EXCLUDED.description_length, EXCLUDED.imgs_qty)
            ;
            """

            common.caricamento_barra(df, cur, sql)
            conn.commit()



def load(df):
    categories.transform(df, "category")
    convert(df)
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    df = df[["product_id", "category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty", "last_updated"]]
    with psycopg.connect(host=host,
                         dbname=dbname,
                         user=user,
                         password=password,
                         port=port) as conn:
        with conn.cursor() as cur:
            # Creazione della tabella nel database
            sql = """
               CREATE TABLE products (
                   pk_product VARCHAR PRIMARY KEY,
                   fk_category VARCHAR,
                   name_length INTEGER,
                   description_length INTEGER,
                   imgs_qty INTEGER,
                   last_updated TIMESTAMP
               );
               """
            try:
                cur.execute(sql)
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
                    cur.execute(sql)

            sql = """
            INSERT INTO products
            (pk_product, fk_category, name_length, description_length, imgs_qty, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (pk_product) DO UPDATE SET 
            (fk_category, name_length, description_length, imgs_qty, last_updated) = (EXCLUDED.fk_category,
            EXCLUDED.name_length, EXCLUDED.description_length, EXCLUDED.imgs_qty, EXCLUDED.last_updated)
            ;
            """

            common.caricamento_barra(df, cur, sql)
            conn.commit()