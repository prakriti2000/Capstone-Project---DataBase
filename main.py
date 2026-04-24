import os
from pyspark.sql import SparkSession
from src.cleaning.clean_claims import clean_claims
from src.cleaning.clean_subscribers import clean_subscribers
from src.cleaning.clean_patients import clean_patients
from src.cleaning.clean_group_subgroup import clean_group_subgroup
from src.transformation.transformations import (
    load_cleaned_data,
    total_rejected_claims,
    claims_by_city,
    save_output,
    disease_max_claims,
    most_subscribed_subgroups,
    average_claim_amount
)


def main():
    spark = SparkSession.builder \
        .appName("HealthcareInsuranceBigDataProject") \
        .master("local[*]") \
        .getOrCreate()

    project_root = os.getcwd()
    base_input = os.path.join(project_root, "Data", "Raw")
    base_cleaned = os.path.join(project_root, "Data", "Cleaned")
    base_output = os.path.join(project_root, "Data", "outputs")

    # Cleaning
    clean_claims(
        spark,
        os.path.join(base_input, "claims.json"),
        os.path.join(base_cleaned, "claims")
    )

    clean_subscribers(
        spark,
        os.path.join(base_input, "subscriber.csv"),
        os.path.join(base_cleaned, "subscribers")
    )

    clean_patients(
        spark,
        os.path.join(base_input, "Patient_records.csv"),
        os.path.join(base_cleaned, "patients")
    )

    clean_group_subgroup(
        spark,
        os.path.join(base_input, "grpsubgrp.csv"),
        os.path.join(base_cleaned, "group_subgroup")
    )

    # Load cleaned data
    claims, subscribers, patients, group_subgroup = load_cleaned_data(spark, base_cleaned)

    # Transformations
    rejected = total_rejected_claims(claims)
    city_claims = claims_by_city(claims, patients)
    disease_max = disease_max_claims(claims)
    subgroup_popularity = most_subscribed_subgroups(group_subgroup)
    avg_claim = average_claim_amount(claims)

    # Save outputs
    save_output(rejected, os.path.join(base_output, "rejected_claims"))
    save_output(city_claims, os.path.join(base_output, "claims_by_city"))
    save_output(disease_max, os.path.join(base_output, "disease_max_claims"))
    save_output(subgroup_popularity, os.path.join(base_output, "subgroup_popularity"))
    save_output(avg_claim, os.path.join(base_output, "avg_claim_amount"))

    spark.stop()
    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()