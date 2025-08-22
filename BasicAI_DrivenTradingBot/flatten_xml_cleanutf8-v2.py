import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import StructType, ArrayType

def pivot_array_column(df, array_col_name, pivot_prefix, max_items, is_struct=False, struct_fields=None):
    """
    Pivots a single array column in a DataFrame into multiple columns.

    :param df: The input DataFrame.
    :param array_col_name: The name of the array column to pivot.
    :param pivot_prefix: The prefix for the new pivoted column names.
    :param max_items: The maximum number of array elements to pivot.
    :param is_struct: Boolean, True if the array contains structs, False otherwise.
    :param struct_fields: A list of tuples for struct fields to extract, e.g., [('_stars', 'stars'), ...].
    :return: DataFrame with the array column pivoted.
    """
    temp_df = df
    print(f"Pivoting column '{array_col_name}' with prefix '{pivot_prefix}'...")
    for i in range(max_items):
        item = col(array_col_name).getItem(i)
        if is_struct and struct_fields:
            # Handle arrays of complex structs
            for field_info in struct_fields:
                original_field_name, new_field_suffix = field_info
                temp_df = temp_df.withColumn(
                    f"{pivot_prefix}_{new_field_suffix}-{i+1}",
                    item.getItem(original_field_name).cast("string")
                )
        else:
            # Handle arrays of simple types (like string)
            temp_df = temp_df.withColumn(
                f"{pivot_prefix}-{i+1}",
                item.cast("string")
            )
    return temp_df

# --- Main Logic for Colab ---
spark = SparkSession.builder \
    .appName("XML Multi-Pivot in Colab") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.17.0") \
    .getOrCreate()

df = spark.read.format("com.databricks.spark.xml").option("rowTag", "book").load("books.xml")

# --- 4. Select all columns, including the new array columns ---
base_df = df.select(
    col("_id"),
    col("title"),
    col("genre"),
    col("price"),
    col("publish_date"),
    col("author.name.first").alias("author_name_first"),
    col("author.name.last").alias("author_name_last"),
    col("reviewers.reviewer").alias("reviewers_array"),
    col("onemorecolumn.col1").alias("onemorecol_array"),
    col("secondcoloum.col1").alias("secondcol_arrry")
)

# --- 5. Define pivot limits and apply pivoting for each array ---
MAX_REVIEWERS = 4
MAX_ONEMORECOL = 2
MAX_SECONDCOL = 3

# Pivot the reviewers array (which is an array of structs)
pivoted_df = pivot_array_column(
    df=base_df,
    array_col_name="reviewers_array",
    pivot_prefix="reviewer",
    max_items=MAX_REVIEWERS,
    is_struct=True,
    struct_fields=[('_stars', 'stars'), ('comment', 'comment'), ('name', 'name')]
)

# Pivot the first new array (a simple array of strings)
pivoted_df = pivot_array_column(
    df=pivoted_df,
    array_col_name="onemorecol_array",
    pivot_prefix="onemorecol",
    max_items=MAX_ONEMORECOL
)

# Pivot the second new array (a simple array of strings)
pivoted_df = pivot_array_column(
    df=pivoted_df,
    array_col_name="secondcol_arrry",
    pivot_prefix="secondcol",
    max_items=MAX_SECONDCOL
)

# Drop the intermediate array columns
final_df = pivoted_df.drop("reviewers_array", "onemorecol_array", "secondcol_arrry")

# --- 6. Clean all string columns ---
string_columns = [c[0] for c in final_df.dtypes if c[1] == 'string']
for column_name in string_columns:
    final_df = final_df.withColumn(
        column_name,
        regexp_replace(col(column_name), "[^\\x00-\\x7F]", " ")
    ).withColumn(
        column_name,
        regexp_replace(col(column_name), "[\\r\\n]", " ")
    )

# --- 7. Replace nulls with empty strings ---
final_df = final_df.na.fill("")

# --- 8. Write to a single TSV file ---
final_df.coalesce(1).write \
    .option("header", "true") \
    .option("sep", "\t") \
    .mode("overwrite") \
    .csv("output_tsv_multi_pivot")

print("Successfully wrote cleaned TSV file with multiple pivots.")
final_df.show(truncate=False, vertical=True)

spark.stop()