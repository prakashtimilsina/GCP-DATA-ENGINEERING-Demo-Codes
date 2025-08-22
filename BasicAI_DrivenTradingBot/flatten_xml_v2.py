# Pivotted values in same rec. Not normalized.
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, ArrayType

# --- Main Logic for Colab ---

# 1. Define file paths
input_path = "/books.xml"
output_path = "output_tsv_pivoted"

# 2. Initialize Spark Session
spark = SparkSession.builder \
    .appName("XML Pivot in Colab") \
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

# --- 4. Process and Pivot the Data ---
print("\nPivoting the reviewer data...")

base_df = df.select(
    col("_id"),
    col("title"),
    col("genre"),
    col("price"),
    col("publish_date"),
    col("author.name.first").alias("author_name_first"),
    col("author.name.last").alias("author_name_last"),
    col("reviewers.reviewer").alias("reviewers_array")
)

MAX_REVIEWERS = 4
pivoted_df = base_df
for i in range(MAX_REVIEWERS):
    reviewer_struct = col("reviewers_array").getItem(i)
    pivoted_df = pivoted_df.withColumn(
        f"reviewers_reviewer__stars-{i+1}",
        reviewer_struct.getItem("_stars")
    ).withColumn(
        f"reviewers_reviewer_comment-{i+1}",
        reviewer_struct.getItem("comment")
    ).withColumn(
        f"reviewers_reviewer_name-{i+1}",
        reviewer_struct.getItem("name")
    )

final_df = pivoted_df.drop("reviewers_array")

print("DataFrame pivoted successfully. Final Schema:")
final_df.printSchema()

print("\nPreview of pivoted data:")
final_df.show(truncate=False)

# Replace all nulls in string columns with empty strings
final_df = final_df.na.fill("")

# --- 5. Write to a single TSV file ---
print(f"\nWriting pivoted data to a single TSV file at: {output_path}")
final_df.coalesce(1).write \
    .option("header", "true") \
    .option("sep", "\t") \
    .mode("overwrite") \
    .csv(output_path)

print("Successfully wrote pivoted TSV file.")

# --- 6. Stop the Spark Session ---
spark.stop()