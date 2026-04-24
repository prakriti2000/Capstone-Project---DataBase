from pyspark.sql.functions import col, to_date
from src.utils.common_cleaning import (
    standardize_column_names,
    trim_string_columns,
    replace_string_nan_with_null,
    fill_null_string_columns,
    drop_duplicates,
    count_nulls,
    standardize_sub_id
)


def clean_claims(spark, input_path, output_path):
    df = spark.read.option("multiline", "true").json(f"file://{input_path}")

    print("Raw Claims Data")
    df.show(5)
    df.printSchema()

    df = standardize_column_names(df)
    df = trim_string_columns(df)
    df = replace_string_nan_with_null(df)
    df = standardize_sub_id(df, "sub_id")

    if "claim_amount" in df.columns:
        df = df.withColumn("claim_amount", col("claim_amount").cast("double"))

    if "claim_date" in df.columns:
        df = df.withColumn("claim_date", to_date(col("claim_date")))

    df = fill_null_string_columns(df, "NA")
    df = drop_duplicates(df)

    print("Null Counts in Claims")
    count_nulls(df).show()

    print("Cleaned Claims Data")
    df.show(5)

    df.write.mode("overwrite").parquet(f"file://{output_path}")
    print("Claims cleaned successfully")