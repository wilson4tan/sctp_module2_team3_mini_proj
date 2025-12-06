import duckdb
import os

# Connect to DuckDB file
con = duckdb.connect("db/mini_proj.db")

# CSV files to load
csv_files = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
    "vgsales.csv"
]

for file in csv_files:
    table_name = os.path.splitext(os.path.basename(file))[0]

    print(f"Loading {file} -> table {table_name}")
    con.sql(
        f"""
        CREATE TABLE {table_name} AS 
        SELECT * FROM read_csv_auto('db/{file}', HEADER=True);
        """
    )

print("All CSV files successfully loaded!")
