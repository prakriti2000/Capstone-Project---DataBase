from pyspark.sql.functions import col, trim, when, count
from pyspark.sql.types import StringType


def standardize_column_names(df):
    for old_col in df.columns:
        new_col = old_col.strip().lower().replace(" ", "_").replace("-", "_")
        df = df.withColumnRenamed(old_col, new_col)
    return df


def trim_string_columns(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))
    return df


def replace_string_nan_with_null(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                when(
                    (trim(col(field.name)) == "") |
                    (col(field.name) == "NaN") |
                    (col(field.name) == "nan") |
                    (col(field.name) == "NULL") |
                    (col(field.name) == "null"),
                    None
                ).otherwise(col(field.name))
            )
    return df


def fill_null_string_columns(df, fill_value="NA"):
    fill_dict = {}
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            fill_dict[field.name] = fill_value
    return df.fillna(fill_dict)


def count_nulls(df):
    return df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])


def drop_duplicates(df):
    return df.dropDuplicates()


def standardize_sub_id(df, column_name="sub_id"):
    if column_name in df.columns:
        df = df.withColumn(column_name, trim(col(column_name)))
    return df