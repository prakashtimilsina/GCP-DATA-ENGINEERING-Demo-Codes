import xml.etree.ElementTree as ET
import csv
import sys

def clean_text(text):
    """Remove unwanted control/non-UTF8 characters."""
    if text is None:
        return ""
    import re
    return re.sub(r'[^\x20-\x7E]+', '', text)

def parse_large_xml(input_file, output_file):
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        # Define the CSV header
        fieldnames = [
            'order_id',
            'customer_name',
            'customer_email',
            'notes',
            'item_number',
            'product',
            'qty'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Set up streaming XML parsing
        context = ET.iterparse(input_file, events=('end',))
        for event, elem in context:
            if elem.tag == 'order':
                # Extract order-level fields
                order_id = elem.findtext('id', default='').strip()
                customer_elem = elem.find('customer')
                customer_name = customer_elem.findtext('name', default='').strip() if customer_elem is not None else ''
                customer_email = customer_elem.findtext('email', default='').strip() if customer_elem is not None else ''
                notes = clean_text(elem.findtext('notes', default='').strip())

                # Extract and flatten items
                items_elem = elem.find('items')
                if items_elem is not None:
                    for idx, item_elem in enumerate(items_elem.findall('item'), start=1):
                        product = item_elem.findtext('product', default='').strip()
                        qty = item_elem.findtext('qty', default='').strip()
                        writer.writerow({
                            'order_id': order_id,
                            'customer_name': customer_name,
                            'customer_email': customer_email,
                            'notes': notes,
                            'item_number': idx,
                            'product': product,
                            'qty': qty
                        })
                else:
                    # Write row even if there are no items (optional)
                    writer.writerow({
                        'order_id': order_id,
                        'customer_name': customer_name,
                        'customer_email': customer_email,
                        'notes': notes,
                        'item_number': '',
                        'product': '',
                        'qty': ''
                    })
                # Free memory
                elem.clear()

if __name__ == '__main__':
    # Usage: python script.py input.xml output.csv
    if len(sys.argv) != 3:
        print("Usage: python script.py input.xml output.csv")
        sys.exit(1)
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    parse_large_xml(input_file, output_file)
    print(f"Done! Output written to {output_file}")

'''
1. Directly Running as a Python Script (Driver-Only)
	•	If you run the script as-is in a Databricks notebook cell (using `%python`), it will execute only on the driver node, not distributed across the Spark cluster.
	•	This is because the script uses standard Python file I/O and `xml.etree.ElementTree`, which are not Spark-distributed operations.
Use case:
	•	Suitable for moderate file sizes (a few GBs).
	•	Not recommended for huge files (100+ GB) as it will hit driver memory/disk limits.
2. Distributed XML Parsing with Spark
	•	Spark does not natively parallelize parsing of a single large XML file.
	•	If you split your XML into many smaller files (see previous advice), Spark can read and process them in parallel using its XML data source.
Example for distributed XML parsing in Databricks:
###### After splitting your large XML into many smaller files ######
df = spark.read.format("xml") \
    .option("rowTag", "order") \
    .load("dbfs:/FileStore/orders_split/*.xml")

df.display()
##################################################
        You can then use Spark DataFrame operations to flatten, transform, and write to Delta/Parquet/CSV.
3. Hybrid Approach: Use Python for Flattening, Spark for Analytics
	•	Use the provided script to flatten the XML to CSV (either locally or on the Databricks driver).
	•	Upload the resulting CSV to DBFS or cloud storage.
	•	Use Spark to read the CSV and perform analytics, pivoting, or writing to Delta.
Example:
# After running the Python script and uploading orders_flat.csv to DBFS
df = spark.read.csv("dbfs:/FileStore/orders_flat.csv", header=True, inferSchema=True)
df.display()

4. Best Practice for Huge Files
	•	Split the XML externally (on a VM, or with a Databricks init script if needed).
	•	Upload the split files to DBFS or cloud storage.
	•	Process with Spark for full cluster parallelism.
Summary Table:
Scenario: Small/Medium XML (driver fits), Recommendation Approach: Run script as-is in Databricks notebook cell
Scenario: Huge XML (>10GB, 100+GB), Recommendation Approach: Split XML, then use Spark DataFrame API
Scenario: Need distributed parsing, Recommendation Approach: Split XML, then use Spark XML reader
Scenario: Flattening only (not analytics), Recommendation Approach: Python script on driver, then Spark for analytics.

Conclusion
	•	You can run the script in Databricks, but for very large files, use it only for flattening on the driver or, better, split the XML first and use Spark for distributed processing.
	•	For true distributed XML parsing and analytics, always split your XML into many files and use Spark’s DataFrame API.

'''
