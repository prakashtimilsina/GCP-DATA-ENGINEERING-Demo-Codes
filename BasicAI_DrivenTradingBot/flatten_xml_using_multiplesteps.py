'''
Here is a complete Databricks notebook workflow for handling very large XML files, including:
	1.	Splitting a large XML file into smaller files (Python script, run locally or on Databricks driver)
	2.	Uploading split files to DBFS
	3.	Reading XML files with Spark
	4.	Flattening and pivoting the data
	5.	Writing the result to a Delta table
Databricks Notebook: Large XML ETL Workflow

1. Split Large XML File into Smaller Files
		Note:
This step is best done locally or on a VM before uploading to Databricks.
If you want to run it on Databricks, use `%python` and ensure the input/output paths are accessible (e.g., `/dbfs/FileStore/...`).

# %python

import xml.etree.ElementTree as ET
import os

input_file = '/dbfs/FileStore/large.xml'  # Path to your large XML file
output_dir = '/dbfs/FileStore/orders_split'  # Output dir for split files
chunk_size = 1000  # Orders per split file

os.makedirs(output_dir, exist_ok=True)

context = ET.iterparse(input_file, events=('end',))
orders = []
file_idx = 1

for event, elem in context:
    if elem.tag == 'order':
        orders.append(ET.tostring(elem, encoding='unicode'))
        if len(orders) == chunk_size:
            with open(f'{output_dir}/orders_part_{file_idx}.xml', 'w', encoding='utf-8') as f:
                f.write(f'<orders>\n{"".join(orders)}\n</orders>')
            orders = []
            file_idx += 1
        elem.clear()

if orders:
    with open(f'{output_dir}/orders_part_{file_idx}.xml', 'w', encoding='utf-8') as f:
        f.write(f'<orders>\n{"".join(orders)}\n</orders>')

2. Upload Split Files to DBFS
	•	If you split locally, use Databricks UI or `databricks-cli` to upload files to `/FileStore/orders_split/`.
	•	If you split on Databricks, files are already in DBFS.
3. Read XML Files with Spark:
from pyspark.sql import functions as F

# Path to split XML files
xml_path = "dbfs:/FileStore/orders_split/*.xml"

# Read XML with rowTag 'order'
df = spark.read.format("xml") \
    .option("rowTag", "order") \
    .load(xml_path)

df.printSchema()
df.display()

4. Flatten and Pivot the Data:
from pyspark.sql.window import Window

# Flatten nested structure
df_flat = df \
    .withColumn("order_id", F.col("id")) \
    .withColumn("customer_name", F.col("customer.name")) \
    .withColumn("customer_email", F.col("customer.email")) \
    .withColumn("notes", F.col("notes")) \
    .withColumn("item", F.explode_outer("items.item")) \
    .withColumn("product", F.col("item.product")) \
    .withColumn("qty", F.col("item.qty")) \
    .select(
        "order_id", "customer_name", "customer_email", "product", "qty", "notes"
    )

# Add row number per item in each order
windowSpec = Window.partitionBy("order_id").orderBy(F.lit(1))
df_numbered = df_flat.withColumn("prod_num", F.row_number().over(windowSpec))

# Prepare pivot columns (Product1, Qty1, Product2, Qty2, ...)
max_products = 10  # Adjust as needed
cols = []
for i in range(1, max_products + 1):
    cols.append(F.max(F.when(F.col("prod_num") == i, F.col("product"))).alias(f"Product{i}"))
    cols.append(F.max(F.when(F.col("prod_num") == i, F.col("qty"))).alias(f"Qty{i}"))

# Group and aggregate
df_pivot = df_numbered.groupBy("order_id", "customer_name", "customer_email", "notes").agg(*cols)

# Select columns in desired order
select_cols = ["order_id", "customer_name", "customer_email"]
for i in range(1, max_products + 1):
    select_cols.append(f"Product{i}")
    select_cols.append(f"Qty{i}")
select_cols.append("notes")

df_pivot = df_pivot.select(*select_cols)
df_pivot.display()

5. Write to Delta Table:
df_pivot.write.format("delta").mode("overwrite").saveAsTable("orders_pivoted")


Summary
	•	Split the large XML file into smaller files for parallel processing.
	•	Upload split files to DBFS.
	•	Read with Spark’s XML reader.
	•	Flatten and pivot the data with Spark SQL.
	•	Write to a Delta table for analytics.
Tips
	•	Adjust `chunk_size` and `max_products` based on your data.
	•	For more than 10 products per order, increase `max_products`.
	•	If your XML structure changes, update the flattening logic accordingly.


'''