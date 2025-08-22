# # install PySpark Since we are running in Collab notebook for testing.
# When Running in dataproc cluster we can use databrick spark-xml library in spark Session directly
#!pip install -q pyspark
# Find location of spark installation
# !pip install -q findspark



## Main Code Starts Here ##
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, ArrayType

def flatten_df(nested_df):
    """
    Recursively flattens a nested PySpark DataFrame.
    This version correctly handles arrays of structs.
    """
    # Keep track of columns to process
    stack = [((), nested_df)]
    # This will be our final flattened DataFrame
    flat_df = nested_df.sql_ctx.createDataFrame(nested_df.sql_ctx._sc.emptyRDD(), nested_df.schema)

    while len(stack) > 0:
        parents, df_to_process = stack.pop()
        
        # Get flat and complex columns
        flat_cols = [
            col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df_to_process.dtypes if not c[1].startswith('struct') and not c[1].startswith('array')
        ]
        
        nested_cols = [
            c[0] for c in df_to_process.dtypes if c[1].startswith('struct')
        ]
        
        array_cols = [
            c[0] for c in df_to_process.dtypes if c[1].startswith('array')
        ]

        # Add flat columns to the final DataFrame
        current_flat_df = df_to_process.select(flat_cols)

        # Process nested structs and add them to the stack
        for nested_col_name in nested_cols:
            nested_df_to_add = df_to_process.select(f"{nested_col_name}.*")
            stack.append((parents + (nested_col_name,), nested_df_to_add))

        # Process arrays, explode them, and add to the stack
        for array_col_name in array_cols:
            # THE FIX IS HERE: We alias the exploded column back to its original name
            exploded_df = df_to_process.withColumn(array_col_name, explode(col(array_col_name))).alias(array_col_name)
            stack.append((parents, exploded_df))

        # This part is a simplified way to combine results, more robust for this logic
        if flat_df.count() == 0:
            flat_df = current_flat_df
        else:
            # This join logic would be complex; the logic above avoids it by processing step-by-step
            # For this specific recursive problem, a different flattening strategy is more robust.
            pass # The original logic had a flaw here, let's use a more robust version.

    # A more robust, industry-standard flattening approach is often iterative rather than purely recursive.
    # Let's switch to a clearer iterative approach that avoids this specific error.

# --- A MORE ROBUST AND CORRECTED FLATTEN FUNCTION ---

def flatten_df_robust(df):
    """
    A more robust iterative function to flatten a PySpark DataFrame.
    """
    # Get all complex field names (structs and arrays)
    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if isinstance(field.dataType, (StructType, ArrayType))])
    
    # Loop until no complex fields are left
    while len(complex_fields) != 0:
        # Get the first complex field
        col_name = list(complex_fields.keys())[0]
        print(f"Processing complex field: {col_name}")

        # If it's a struct, expand its fields
        if isinstance(complex_fields[col_name], StructType):
            expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) for k in [n.name for n in  complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)

        # If it's an array, explode it
        elif isinstance(complex_fields[col_name], ArrayType):
            df = df.withColumn(col_name, explode(col_name))

        # Refresh the list of complex fields
        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if isinstance(field.dataType, (StructType, ArrayType))])
    return df


# --- Main Logic for Colab ---

# 1. Define file paths ## We can update path in Google Cloud using buckets
input_path = "/books.xml"
output_path = "output_tsv"

# 2. Initialize Spark Session with the spark-xml package
# This is the key change for running in a notebook.
spark = SparkSession.builder \
    .appName("XML to TSV in Colab") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.17.0") \
    .getOrCreate()

print("Spark Session created successfully.")

# 3. Read the XML file
df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "book") \
    .load(input_path)

print("XML file loaded. Original Schema:")
df.printSchema()

# 4. Flatten the DataFrame
print("\nFlattening the DataFrame...")
flat_df = flatten_df_robust(df)
print("DataFrame flattened. Final Schema:")
flat_df.printSchema()

print("\nPreview of flattened data:")
flat_df.show(5, truncate=False)

# 5. Write to a single TSV file
print(f"\nWriting data to a single TSV file at: {output_path}")
flat_df.coalesce(1).write \
    .option("header", "true") \
    .option("sep", "\t") \
    .mode("overwrite") \
    .csv(output_path)

print("Successfully wrote TSV file.")

# 6. Stop the Spark Session
spark.stop()