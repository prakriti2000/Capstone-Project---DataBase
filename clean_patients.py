from pyspark.sql.functions import col, to_date
from src.utils.common_cleaning import (
    standardize_column_names,
    trim_string_columns,
    replace_string_nan_with_null,
    fill_null_string_columns,
    drop_duplicates,
    count_nulls
)


def clean_patients(spark, input_path, output_path):
    df = spark.read.option("header", True).csv(f"file://{input_path}")

    print("Raw Patients Data")
    df.show(5)
    df.printSchema()

    df = standardize_column_names(df)
    df = trim_string_columns(df)
    df = replace_string_nan_with_null(df)

    for date_col in ["patient_birth_date", "birth_date", "admit_date", "discharge_date"]:
        if date_col in df.columns:
            df = df.withColumn(date_col, to_date(col(date_col)))

    if "patient_id" in df.columns:
        df = df.withColumn("patient_id", col("patient_id").cast("long"))

    df = fill_null_string_columns(df, "NA")
    df = drop_duplicates(df)

    print("Null Counts in Patients")
    count_nulls(df).show()

    print("Cleaned Patients Data")
    df.show(5)

    df.write.mode("overwrite").parquet(f"file://{output_path}")
    print("Patients cleaned successfully")