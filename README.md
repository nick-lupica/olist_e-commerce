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

**sellers**:
* pk_seller VARCHAR
* region VARCHAR

**orders_products**:
* pk_order_product SERIAL
* fk_order VARCHAR
* fk_product VARCHAR
* fk_seller VARCHAR
* price FLOAT
* freight FLOAT


# Power BI Project
Tra i file è presente un report creato con Power BI. Per visualizzarlo, scarica il file `visualization_olist.pbix` e aprilo con Power BI Desktop.

1. Scarica e installa Power BI Desktop da [qui](https://powerbi.microsoft.com/desktop/).
2. Apri il file `visualization_olist.pbix` con Power BI Desktop.
3. Interagisci con il report.
