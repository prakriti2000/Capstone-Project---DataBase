from src.utils.common_cleaning import (
    standardize_column_names,
    trim_string_columns,
    replace_string_nan_with_null,
    fill_null_string_columns,
    drop_duplicates,
    count_nulls
)


def clean_group_subgroup(spark, input_path, output_path):
    df = spark.read.option("header", True).csv(f"file://{input_path}")

    print("Raw Group-Subgroup Data")
    df.show(5)
    df.printSchema()

    df = standardize_column_names(df)
    df = trim_string_columns(df)
    df = replace_string_nan_with_null(df)
    df = fill_null_string_columns(df, "NA")
    df = drop_duplicates(df)

    print("Null Counts in Group-Subgroup")
    count_nulls(df).show()

    print("Cleaned Group-Subgroup Data")
    df.show(5)

    df.write.mode("overwrite").parquet(f"file://{output_path}")
    print("Group-Subgroup cleaned successfully")
