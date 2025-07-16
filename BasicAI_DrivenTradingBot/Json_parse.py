from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer, col
from pyspark.sql.types import ArrayType, StructType

def flatten_df(nested_df):
    """
    Recursively flattens a nested DataFrame.
    Arrays are exploded, structs are expanded to columns.
    """
    flat_cols = []
    explode_cols = []

    for field in nested_df.schema.fields:
        field_name = field.name
        field_type = field.dataType

        if isinstance(field_type, StructType):
            # StructType => expand fields
            for subfield in field_type.fields:
                flat_cols.append(col(f"{field_name}.{subfield.name}").alias(f"{field_name}_{subfield.name}"))
        elif isinstance(field_type, ArrayType):
            # ArrayType => explode outer
            explode_cols.append(field_name)
        else:
            flat_cols.append(col(field_name))

    df_flat = nested_df.select(flat_cols + [col(c) for c in explode_cols])

    for c in explode_cols:
        df_flat = df_flat.withColumn(c, explode_outer(col(c)))

    # If there are still nested columns, recurse
    complex_fields = [field for field in df_flat.schema.fields if isinstance(field.dataType, (ArrayType, StructType))]

    if complex_fields:
        return flatten_df(df_flat)
    else:
        return df_flat


def main():
    spark = SparkSession.builder \
        .appName("RecursiveFlattener") \
        .getOrCreate()

    # Read JSON
    input_path = "large_irregular_nested.json"
    df = spark.read.option("multiline", True).json(input_path)

    print("Original schema:")
    df.printSchema()

    df_flat = flatten_df(df)

    print("Flattened schema:")
    df_flat.printSchema()

    df_flat.write.option("header", True).mode("overwrite").csv("flattened_recursive_output")

    spark.stop()


if __name__ == "__main__":
    main()

'''

Key Benefits

✔️ Fully schema-agnostic.
✔️ Handles deeply nested levels automatically.
✔️ Works with any irregular JSON — no hardcoded field names.
✔️ Outputs a BigQuery-friendly flat CSV.

Bash Command:
spark-submit recursive_flatten.py

Outputs:
flattened_recursive_output/part-*.csv


'''