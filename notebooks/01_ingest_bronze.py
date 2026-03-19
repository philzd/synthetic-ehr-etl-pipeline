"""
Bronze Layer: Ingest raw Synthea CSV data into Parquet format with ingestion timestamps.

Note: This script is intended to run in a Databricks environment where `spark`
and `display()` are available.
"""

from pyspark.sql import functions as F

# --- Patients ---
patients_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/ehr_raw/patients.csv")
    .withColumn("ingest_ts", F.current_timestamp())
)

display(patients_raw)

patients_raw.write.mode("overwrite").parquet(
    "/Volumes/workspace/default/ehr_raw/bronze/patients"
)

# --- Encounters ---
encounters_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/ehr_raw/encounters.csv")
    .withColumn("ingest_ts", F.current_timestamp())
)

encounters_raw.write.mode("overwrite").parquet(
    "/Volumes/workspace/default/ehr_raw/bronze/encounters"
)

encounters_bronze = spark.read.parquet(
    "/Volumes/workspace/default/ehr_raw/bronze/encounters"
)

display(encounters_bronze)

# --- Conditions ---
conditions_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/ehr_raw/conditions.csv")
    .withColumn("ingest_ts", F.current_timestamp())
)

conditions_raw.write.mode("overwrite").parquet(
    "/Volumes/workspace/default/ehr_raw/bronze/conditions"
)

# --- Medications ---
medications_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/Volumes/workspace/default/ehr_raw/medications.csv")
    .withColumn("ingest_ts", F.current_timestamp())
)

medications_raw.write.mode("overwrite").parquet(
    "/Volumes/workspace/default/ehr_raw/bronze/medications"
)
