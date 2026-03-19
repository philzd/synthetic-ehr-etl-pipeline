"""
Silver Layer: Clean and standardize bronze EHR tables into schema-consistent
patient- and encounter-level datasets.

Note: This script is intended to run in a Databricks environment where `spark`
and `display()` are available.
"""

from pyspark.sql import functions as F

# --- Patients ---
patients_bronze = spark.read.parquet(
    "/Volumes/workspace/default/ehr_raw/bronze/patients"
)

patients_silver = (
    patients_bronze
    .select(
        F.col("Id").alias("patient_id"),
        F.col("BIRTHDATE").alias("birth_date"),
        F.col("DEATHDATE").alias("death_date"),
        F.col("GENDER").alias("gender"),
        F.col("RACE").alias("race"),
        F.col("ETHNICITY").alias("ethnicity"),
        F.col("CITY").alias("city"),
        F.col("STATE").alias("state"),
        F.col("COUNTY").alias("county"),
        F.col("ZIP").alias("zip"),
        F.col("ingest_ts")
    )
    .dropDuplicates(["patient_id"])
)

display(patients_silver)

patients_silver.write.mode("overwrite").parquet(
    "/Volumes/workspace/default/ehr_raw/silver/patients"
)

# --- Encounters ---
encounters_bronze = spark.read.parquet(
    "/Volumes/workspace/default/ehr_raw/bronze/encounters"
)

encounters_silver = (
    encounters_bronze
    .select(
        F.col("Id").alias("encounter_id"),
        F.col("PATIENT").alias("patient_id"),
        F.col("ORGANIZATION").alias("organization_id"),
        F.col("ENCOUNTERCLASS").alias("encounter_class"),
        F.col("START").alias("start_ts"),
        F.col("STOP").alias("end_ts"),
        F.col("PAYER_COVERAGE").cast("double").alias("payer_coverage"),
        F.col("REASONCODE").alias("reason_code"),
        F.col("REASONDESCRIPTION").alias("reason_description"),
        F.col("ingest_ts")
    )
    .dropDuplicates(["encounter_id"])
    .withColumn(
        "encounter_duration_minutes",
        (F.col("end_ts").cast("long") - F.col("start_ts").cast("long")) / 60.0
    )
)

display(encounters_silver)

encounters_silver.write.mode("overwrite").parquet(
    "/Volumes/workspace/default/ehr_raw/silver/encounters"
)
