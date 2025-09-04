## Environment Setup ##
!pip install -q pyspark findspark

import json
import logging
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim
from pyspark.sql.types import StructType
from pyspark.errors import AnalysisException

# --- Initialize findspark ---
findspark.init()

# --- Setup logging ---
#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# --- 1. Setup logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='parser.log',  # This will save logs to a file named parser.log
    filemode='w'            # 'w' for overwrite, 'a' for append
)

# --- Define Functions ---
def load_config(config_path):
    """Loads the JSON configuration file with error handling."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file not found at: {config_path}")
        raise
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from the configuration file: {config_path}")
        raise

def generate_and_save_schema(spark, xml_path, row_tag, output_path):
    """Infers schema from XML and saves it to a file."""
    try:
        logging.info(f"Reading sample XML from '{xml_path}' to infer schema...")
        df = spark.read.format("com.databricks.spark.xml").option("rowTag", row_tag).load(xml_path)
        schema_json = df.schema.json()
        with open(output_path, 'w') as f:
            f.write(schema_json)
        logging.info(f"Schema successfully inferred and saved to '{output_path}'")
    except Exception as e:
        logging.error(f"Failed to generate schema: {e}")
        raise

def load_schema(spark, schema_path):
    """Loads a schema from a JSON file with error handling."""
    try:
        with open(schema_path, 'r') as f:
            schema_json = f.read()
        return StructType.fromJson(json.loads(schema_json))
    except FileNotFoundError:
        logging.error(f"Schema file not found at: {schema_path}")
        raise

def pivot_array_column(df, config):
    """Pivots a single array column based on configuration."""
    # (Function content is the same as in the script)
    array_col_name = config['alias']
    pivot_prefix = config['pivot_prefix']
    max_items = config['max_items']
    is_struct = config.get('is_struct', False)
    temp_df = df
    logging.info(f"Pivoting column '{array_col_name}' with prefix '{pivot_prefix}'...")
    for i in range(max_items):
        item = col(array_col_name).getItem(i)
        if is_struct:
            for field in config['struct_fields']:
                temp_df = temp_df.withColumn(
                    f"{pivot_prefix}_{field['new_suffix']}-{i+1}",
                    item.getItem(field['original']).cast("string")
                )
        else:
            temp_df = temp_df.withColumn(f"{pivot_prefix}-{i+1}", item.cast("string"))
    return temp_df

# --- Main Execution Logic ---
spark = None
try:
    # Load Configuration
    config_path = "config.json"
    logging.info(f"Loading configuration from: {config_path}")
    config = load_config(config_path)

    # Initialize Spark Session
    logging.info("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName(config.get("appName", "XML-Parser-Colab")) \
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.17.0") \
        .getOrCreate()
    logging.info("Spark session created successfully.")

    # Generate and save the schema first
    generate_and_save_schema(spark, config['source_xml_path'], config['row_tag'], config['schema_path'])

    # Load Schema and Data
    logging.info(f"Loading schema from: {config['schema_path']}")
    schema = load_schema(spark, config['schema_path'])
    logging.info(f"Reading XML data from: {config['source_xml_path']}")
    df = spark.read.format("com.databricks.spark.xml").option("rowTag", config['row_tag']).schema(schema).load(config['source_xml_path'])
    logging.info("Successfully loaded XML data into DataFrame.")

    # Flatten and Select Columns
    logging.info("Flattening and selecting columns as per configuration...")
    select_exprs = [col(c['path']).alias(c['alias']) for c in config['flatten_cols']]
    for array_config in config['pivot_arrays']:
        select_exprs.append(col(array_config['path']).alias(array_config['alias']))
    base_df = df.select(select_exprs)

    # Apply Pivoting
    pivoted_df = base_df
    for pivot_config in config['pivot_arrays']:
        pivoted_df = pivot_array_column(pivoted_df, pivot_config)
    array_aliases_to_drop = [p['alias'] for p in config['pivot_arrays']]
    final_df = pivoted_df.drop(*array_aliases_to_drop)

    # Clean and Finalize DataFrame
    logging.info("Cleaning and finalizing DataFrame...")
    string_columns = [c[0] for c in final_df.dtypes if c[1] == 'string']
    for column_name in string_columns:
        final_df = final_df.withColumn(column_name, trim(regexp_replace(col(column_name), "[^\\x00-\\x7F]", "")))
        final_df = final_df.withColumn(column_name, trim(regexp_replace(col(column_name), "[\\r\\n]", " ")))
    final_df = final_df.na.fill("")
    logging.info("DataFrame cleaning complete.")

    # Write Output
    logging.info(f"Writing output to: {config['target_path']}")
    final_df.coalesce(1).write.option("header", "true").option("sep", "\t").mode("overwrite").csv(config['target_path'])
    logging.info(f"Successfully processed and wrote data to '{config['target_path']}'")

    print("\n--- Final Output Schema ---")
    final_df.printSchema()
    print("\n--- Final Output Preview (5 rows) ---")
    final_df.show(5, truncate=False)

except Exception as e:
    logging.error(f"An unexpected error occurred in the main process: {e}", exc_info=True)
finally:
    if spark:
        app_name = spark.conf.get("spark.app.name")
        logging.info(f"Stopping the Spark session: '{app_name}'")
        spark.stop()