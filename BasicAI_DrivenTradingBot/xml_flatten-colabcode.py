## Environment Setup ##
!pip install -q pyspark findspark
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim
from os import truncate
# Import all necessary types for schema definition
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
import json

# --- Main Logic for Colab ---
spark = SparkSession.builder \
    .appName("XML-Parser-with-Schema") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.17.0") \
    .getOrCreate()

# Schema Generation
xml_path = '/content/books_v2.xml'
output_path = '/content/output.json'
row_tag = 'book'

"""
    Infers the schema from an XML file and saves it as a JSON file.

    :param spark: The SparkSession object.
    :param xml_path: Path to the sample XML file.
    :param row_tag: The XML tag that defines a single record (e.g., 'book').
    :param output_path: Path to save the generated schema JSON file.
"""
print(f"Reading sample XML from '{xml_path}' to infer schema...")
    
    # Read the XML using the spark-xml library and let Spark infer the schema.
    # The 'rowTag' option is crucial for telling Spark what element represents a single row.
df = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rowTag", row_tag) \
        .load(xml_path)

    # Extract the schema in a portable JSON format.
schema_json = df.schema.json()
df.printSchema()
    # Save the schema to the specified output file.
with open(output_path, 'w') as f:
        f.write(schema_json)
        
print(f"Schema successfully inferred and saved to '{output_path}'")
    
print("\n--- Inferred Schema Structure ---")
df.printSchema()

######
config_path = '/content/config.json' # This is generated for each xml document type for correct mapping of element to columns parsing

def load_config(config_path):
    """Loads the JSON configuration file."""
    with open(config_path, 'r') as f:
        return json.load(f)

def load_schema(spark, schema_path):
    """Loads a schema from a JSON file."""
    with open(schema_path, 'r') as f:
        schema_json = f.read()
    return StructType.fromJson(json.loads(schema_json))

def pivot_array_column(df, config):
    """Pivots a single array column based on configuration."""
    array_col_name = config['alias']
    pivot_prefix = config['pivot_prefix']
    max_items = config['max_items']
    is_struct = config.get('is_struct', False)
    
    temp_df = df
    print(f"Pivoting column '{array_col_name}' with prefix '{pivot_prefix}'...")
    for i in range(max_items):
        item = col(array_col_name).getItem(i)
        if is_struct:
            for field in config['struct_fields']:
                temp_df = temp_df.withColumn(
                    f"{pivot_prefix}_{field['new_suffix']}-{i+1}",
                    item.getItem(field['original']).cast("string")
                )
        else:
            temp_df = temp_df.withColumn(
                f"{pivot_prefix}-{i+1}",
                item.cast("string")
            )
    return temp_df

config = load_config(config_path)

# --- Load Schema and Data ---
schema = load_schema(spark, config['schema_path'])
df = spark.read \
        .format("com.databricks.spark.xml") \
        .option("rowTag", config['row_tag']) \
        .schema(schema) \
        .load(config['source_xml_path'])
print(f"After first parsing without flatten, pivoting and cleanup")
df.show(truncate=False)

# --- Flatten and Select Columns ---
select_exprs = [col(c['path']).alias(c['alias']) for c in config['flatten_cols']]
for array_config in config['pivot_arrays']:
        select_exprs.append(col(array_config['path']).alias(array_config['alias']))
base_df = df.select(select_exprs)
print(f"After flattenning and selectiong columns but no pivoting")
base_df.show(truncate=False)

# --- Apply Pivoting ---
pivoted_df = base_df
for pivot_config in config['pivot_arrays']:
    pivoted_df = pivot_array_column(pivoted_df, pivot_config)
# --- Clean Up and Finalize ---
array_aliases_to_drop = [p['alias'] for p in config['pivot_arrays']]
final_df = pivoted_df.drop(*array_aliases_to_drop)
print(f"After flattenning and pivoting but before cleanup")
final_df.show(truncate=False)

string_columns = [c[0] for c in final_df.dtypes if c[1] == 'string']
for column_name in string_columns:
    final_df = final_df.withColumn(column_name, trim(regexp_replace(col(column_name), "[^\\x00-\\x7F]", "")))
    final_df = final_df.withColumn(column_name, trim(regexp_replace(col(column_name), "[\\r\\n]", " ")))    
final_df = final_df.na.fill("")
print(f"After cleanup of UTF8 and \\n character")
final_df.show(truncate=False)
#display(final_df)
# --- Write Output ---
final_df.coalesce(1).write \
        .option("header", "true") \
        .option("sep", "\t") \
        .mode("overwrite") \
        .csv(config['target_path'])

print(f"Successfully processed and wrote parsed data to '{config['target_path']}'")
print(f"Schema of Output Parsed file ....")
final_df.printSchema()

# Stop the spark session
print(f"Stopping the spark session {spark.conf.get('spark.app.name')} after parsing process is completed.")
spark.stop()