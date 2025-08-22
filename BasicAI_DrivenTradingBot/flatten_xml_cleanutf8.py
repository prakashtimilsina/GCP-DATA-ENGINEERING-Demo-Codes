# removed null to empty, utf8 to empty, \n to " ".
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import StructType, ArrayType

# --- Main Logic for Colab ---

# 1. Define file paths
input_path = "/books.xml"
output_path = "output_tsv_pivoted_cleaned"

# 2. Initialize Spark Session
spark = SparkSession.builder \
    .appName("XML Pivot and Clean in Colab") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.17.0") \
    .getOrCreate()

# 3. Read the XML file
df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "book") \
    .load(input_path)

# --- 4. Process and Pivot the Data ---
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
        reviewer_struct.getItem("_stars").cast("string")  # <-- ADDED CAST
    ).withColumn(
        f"reviewers_reviewer_comment-{i+1}",
        reviewer_struct.getItem("comment").cast("string") # <-- ADDED CAST
    ).withColumn(
        f"reviewers_reviewer_name-{i+1}",
        reviewer_struct.getItem("name").cast("string")    # <-- ADDED CAST
    )

final_df = pivoted_df.drop("reviewers_array")

print("DataFrame pivoted successfully.")
print("\nPreview of pivoted data (before cleaning):")
final_df.show(truncate=False)

# --- 5. Clean all string columns ---
string_columns = [c[0] for c in final_df.dtypes if c[1] == 'string']
for column_name in string_columns:
    final_df = final_df.withColumn(
        column_name,
        regexp_replace(col(column_name), "[^\\x00-\\x7F]", "")
    ).withColumn(
        column_name,
        regexp_replace(col(column_name), "[\\r\\n]", " ")
    )

# --- 6. Replace nulls with empty strings ---
# Now this will work on all reviewer columns because they are all strings.
final_df = final_df.na.fill("")

# --- 7. Write to a single TSV file ---
final_df.coalesce(1).write \
    .option("header", "true") \
    .option("sep", "\t") \
    .mode("overwrite") \
    .csv(output_path)

print("Successfully wrote cleaned TSV file with no nulls.")
print("Preview of cleaned dataset")
final_df.show(truncate=False)


# --- 8. Stop the Spark Session ---
spark.stop()