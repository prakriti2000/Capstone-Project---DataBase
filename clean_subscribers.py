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


def clean_subscribers(spark, input_path, output_path):
    df = spark.read.option("header", True).csv(f"file://{input_path}")

    print("Raw Subscribers Data")
    df.show(5)
    df.printSchema()

    df = standardize_column_names(df)
    df = trim_string_columns(df)
    df = replace_string_nan_with_null(df)

    if "sub_id" not in df.columns:
        for c in df.columns:
            if "sub" in c and "id" in c:
                df = df.withColumnRenamed(c, "sub_id")
                break

    df = standardize_sub_id(df, "sub_id")

    for date_col in ["birth_date", "eff_date", "term_date"]:
        if date_col in df.columns:
            df = df.withColumn(date_col, to_date(col(date_col)))

    if "monthly_premium" in df.columns:
        df = df.withColumn("monthly_premium", col("monthly_premium").cast("double"))

    df = fill_null_string_columns(df, "NA")
    df = drop_duplicates(df)

    print("Null Counts in Subscribers")
    count_nulls(df).show()

    print("Cleaned Subscribers Data")
    df.show(5)

    df.write.mode("overwrite").parquet(f"file://{output_path}")
    print("Subscribers cleaned successfully")