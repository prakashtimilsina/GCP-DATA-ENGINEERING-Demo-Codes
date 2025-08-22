import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
# Import all necessary types for schema definition
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType

def pivot_array_column(df, array_col_name, pivot_prefix, max_items, is_struct=False, struct_fields=None):
    """
    Pivots a single array column in a DataFrame into multiple columns.
    """
    temp_df = df
    print(f"Pivoting column '{array_col_name}' with prefix '{pivot_prefix}'...")
    for i in range(max_items):
        item = col(array_col_name).getItem(i)
        if is_struct and struct_fields:
            for field_info in struct_fields:
                original_field_name, new_field_suffix = field_info
                temp_df = temp_df.withColumn(
                    f"{pivot_prefix}_{new_field_suffix}-{i+1}",
                    item.getItem(original_field_name).cast("string")
                )
        else:
            temp_df = temp_df.withColumn(
                f"{pivot_prefix}-{i+1}",
                item.cast("string")
            )
    return temp_df

# --- 1. Define the Schema for the XML Document ---
reviewer_schema = StructType([
    StructField("_stars", LongType(), True),
    StructField("name", StringType(), True),
    StructField("comment", StringType(), True)
])

book_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("author", StructType([
        StructField("name", StructType([
            StructField("first", StringType(), True),
            StructField("last", StringType(), True)
        ]), True)
    ]), True),
    StructField("title", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("publish_date", StringType(), True),
    StructField("reviewers", StructType([
        StructField("reviewer", ArrayType(reviewer_schema), True)
    ]), True),
    StructField("onemorecolumn", StructType([
        StructField("col1", ArrayType(StringType()), True)
    ]), True),
    StructField("secondcoloum", StructType([
        StructField("col1", ArrayType(StringType()), True)
    ]), True)
])


# --- Main Logic for Colab ---
spark = SparkSession.builder \
    .appName("XML Multi-Pivot with Schema") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.17.0") \
    .getOrCreate()

# --- 2. Read XML using the predefined schema ---
df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "book") \
    .schema(book_schema) \
    .load("/books.xml")

print("DataFrame pivoted successfully.")
print("\nPreview of pivoted data (before cleaning):")
df.show(truncate=False)

# --- 3. Select all columns, including the new array columns ---
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


# --- 4. Define pivot limits and apply pivoting for each array ---
MAX_REVIEWERS = 4
MAX_ONEMORECOL = 2
MAX_SECONDCOL = 3

pivoted_df = pivot_array_column(
    df=base_df,
    array_col_name="reviewers_array",
    pivot_prefix="reviewer",
    max_items=MAX_REVIEWERS,
    is_struct=True,
    struct_fields=[('_stars', 'stars'), ('comment', 'comment'), ('name', 'name')]
)
pivoted_df = pivot_array_column(df=pivoted_df, array_col_name="onemorecol_array", pivot_prefix="onemorecol", max_items=MAX_ONEMORECOL)
pivoted_df = pivot_array_column(df=pivoted_df, array_col_name="secondcol_arrry", pivot_prefix="secondcol", max_items=MAX_SECONDCOL)

final_df = pivoted_df.drop("reviewers_array", "onemorecol_array", "secondcol_arrry")

# --- 5. Clean and Finalize the DataFrame ---
string_columns = [c[0] for c in final_df.dtypes if c[1] == 'string']
for column_name in string_columns:
    final_df = final_df.withColumn(column_name, regexp_replace(col(column_name), "[^\\x00-\\x7F]", "")).withColumn(column_name, regexp_replace(col(column_name), "[\\r\\n]", " "))
final_df = final_df.na.fill("")

# --- 6. Write to a single TSV file ---
final_df.coalesce(1).write \
    .option("header", "true") \
    .option("sep", "\t") \
    .mode("overwrite") \
    .csv("output_tsv_multi_pivot")

print("Successfully wrote cleaned TSV file using a predefined schema.")
final_df.printSchema()
final_df.show(truncate=False, vertical=False)

spark.stop()