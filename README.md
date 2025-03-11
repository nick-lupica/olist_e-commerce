# Progetto e-commerce OLIST

Tabelle a disposizione:

**customers**:
* pk_customer VARCHAR
* region char VARCHAR
* city char VARCHAR
* cap char VARCHAR

**categories**:
* pk_category SERIAL
* name VARCHAR

**products**:
* pk_product VARCHAR
* fk_category INTEGER
* name_length INTEGER
* description_length INTEGER
* imgs_qty INTEGER

**orders**:
* pk_order VARCHAR
* fk_customer VARCHAR
* status VARCHAR
* purchase_timestamp TIMESTAMP
* delivered_timestamp TIMESTAMP
* estimated_date DATE

**sellers** (ancora non disponibile):
* pk_seller VARCHAR
* region VARCHAR

**orders_products**:
* pk_order_product SERIAL
* fk_order VARCHAR
* fk_product VARCHAR
* fk_seller VARCHAR
* price FLOAT
* freight FLOAT


## TODO
- Salvare il file dopo l'estrazione impostando data e orario come valore univoco.
- Creare database da pycharm