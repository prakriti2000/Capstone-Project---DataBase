from pyspark.sql.functions import col
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    count,
    avg,
    desc,
    current_date,
    floor,
    months_between,
    lower
)


def load_cleaned_data(spark: SparkSession, base_path: str) -> dict:
    """
    Load cleaned datasets from parquet output folders.
    """
    data = {
        "claims": spark.read.parquet(f"{base_path}/claims"),
        "subscribers": spark.read.parquet(f"{base_path}/subscribers"),
        "patients": spark.read.parquet(f"{base_path}/patients"),
        "group_subgroup": spark.read.parquet(f"{base_path}/group_subgroup"),
    }
    return data


def add_age_column(df: DataFrame, birth_date_col: str, age_col_name: str = "age") -> DataFrame:
    """
    Add age column from a birth date field.
    """
    if birth_date_col in df.columns:
        return df.withColumn(age_col_name, floor(months_between(current_date(), col(birth_date_col)) / 12))
    return df


def disease_max_claims(claims_df: DataFrame) -> DataFrame:
    return (
        claims_df.groupBy("disease_name")
        .agg(count("*").alias("total_claims"))
        .orderBy(desc("total_claims"))
    )


def subscribers_under_30_with_subgroup(subscribers_df: DataFrame) -> DataFrame:
    df = subscribers_df

    birth_col = None
    for candidate in ["birth_date", "subscriber_birth_date", "dob"]:
        if candidate in df.columns:
            birth_col = candidate
            break

    if birth_col:
        df = add_age_column(df, birth_col, "age")
        return df.filter((col("age") < 30) & (col("subgrp_id").isNotNull()) & (col("subgrp_id") != "NA"))

    return df.limit(0)


def group_max_subgroups(group_subgroup_df: DataFrame) -> DataFrame:
    grp_col = None
    subgrp_col = None

    for c in group_subgroup_df.columns:
        if c.lower() in ["grp_id", "group_id"]:
            grp_col = c
        if c.lower() in ["subgrp_id", "subgroup_id"]:
            subgrp_col = c

    if grp_col and subgrp_col:
        return (
            group_subgroup_df.groupBy(col(grp_col).alias("grp_id"))
            .agg(count(col(subgrp_col)).alias("subgroup_count"))
            .orderBy(desc("subgroup_count"))
        )

    return group_subgroup_df.limit(0)


def most_subscribed_subgroups(subscribers_df: DataFrame) -> DataFrame:
    if "subgrp_id" in subscribers_df.columns:
        return (
            subscribers_df.filter(col("subgrp_id").isNotNull() & (col("subgrp_id") != "NA"))
            .groupBy("subgrp_id")
            .agg(count("*").alias("subscription_count"))
            .orderBy(desc("subscription_count"))
        )

    return subscribers_df.limit(0)


def total_rejected_claims(claims_df: DataFrame) -> DataFrame:
    """
    Assumption:
    N = rejected
    Y = accepted
    NA = unknown
    """
    if "claim_or_rejected" in claims_df.columns:
        return claims_df.filter(lower(col("claim_or_rejected")) == "n").agg(
            count("*").alias("rejected_claim_count")
        )

    return claims_df.limit(0)


def claims_by_city(claims_df: DataFrame, patients_df: DataFrame) -> DataFrame:
    """
    Join claims with patients to find city with most claims.
    """
    patient_city_col = None
    for c in patients_df.columns:
        if c.lower() == "city":
            patient_city_col = c
            break

    if "patient_id" in claims_df.columns and "patient_id" in patients_df.columns and patient_city_col:
        joined = claims_df.join(patients_df, on="patient_id", how="inner")
        return (
            joined.groupBy(col(patient_city_col).alias("city"))
            .agg(count("*").alias("total_claims"))
            .orderBy(desc("total_claims"))
        )

    return claims_df.limit(0)


def average_monthly_premium(subscribers_df: DataFrame) -> DataFrame:
    """
    This works only if monthly_premium exists in subscriber data.
    """
    if "monthly_premium" in subscribers_df.columns:
        return subscribers_df.agg(avg(col("monthly_premium")).alias("avg_monthly_premium"))

    return subscribers_df.limit(0)


def cancer_patients_under_18(patients_df: DataFrame) -> DataFrame:
    birth_col = None
    disease_col = None

    for c in patients_df.columns:
        if c.lower() in ["patient_birth_date", "birth_date", "dob"]:
            birth_col = c
        if c.lower() in ["disease_name", "disease"]:
            disease_col = c

    if birth_col and disease_col:
        df = add_age_column(patients_df, birth_col, "age")
        return df.filter(
            (col("age") < 18) &
            (lower(col(disease_col)).contains("cancer"))
        )

    return patients_df.limit(0)

def load_cleaned_data(spark, base_path):
    claims = spark.read.parquet(f"file://{base_path}/claims")
    subscribers = spark.read.parquet(f"file://{base_path}/subscribers")
    patients = spark.read.parquet(f"file://{base_path}/patients")
    group_subgroup = spark.read.parquet(f"file://{base_path}/group_subgroup")

    return claims, subscribers, patients, group_subgroup

from pyspark.sql.functions import col

def total_rejected_claims(claims_df):
    result = claims_df.filter(col("Claim_Or_Rejected") == "Rejected")
    return result.groupBy().count().withColumnRenamed("count", "total_rejected_claims")

def claims_by_city(claims_df, patients_df):
    joined_df = claims_df.join(patients_df, on="patient_id", how="inner")

    return joined_df.groupBy("city") \
        .count() \
        .withColumnRenamed("count", "total_claims")

from pyspark.sql.functions import avg

def average_claim_amount(claims_df):
    return claims_df.select(avg("claim_amount").alias("avg_claim_amount"))

def save_output(df: DataFrame, output_path: str):
    df.write.mode("overwrite").parquet(f"file://{output_path}")