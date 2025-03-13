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
- copia del file in input alla cartella raw 
(fare in modo che il nome del file sia univoco, 
con data e ora)
- prima di fare il load creare database da Python
- controllo di validità per cancellare la tabella
(con user e psw)
- metodo per controllo di validità degli input,
oltre a strip() e upper()/lower()

- check sulle 20 regioni ammesse
- integrare dati customer a partire dal cap
- gestione del tipo di valore da aggiornare in fillNulls