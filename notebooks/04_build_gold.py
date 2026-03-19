"""
Gold Layer: Build healthcare analytics-ready outputs from silver EHR tables,
including patient-level summary metrics and condition aggregates.

Note: This script is intended to run in a Databricks environment where `spark`
and `display()` are available.
"""

from pyspark.sql import functions as F

patients_silver = spark.read.parquet(
    "/Volumes/workspace/default/ehr_raw/silver/patients"
)

encounters_silver = spark.read.parquet(
    "/Volumes/workspace/default/ehr_raw/silver/encounters"
)

# --- Conditions ---
conditions_bronze = spark.read.parquet(
    "/Volumes/workspace/default/ehr_raw/bronze/conditions"
)

conditions_silver = (
    conditions_bronze
    .select(
        F.col("PATIENT").alias("patient_id"),
        F.col("ENCOUNTER").alias("encounter_id"),
        F.col("CODE").alias("condition_code"),
        F.col("DESCRIPTION").alias("condition_description"),
        F.col("START").alias("start_date"),
        F.col("STOP").alias("stop_date"),
        F.col("ingest_ts")
    )
    .dropDuplicates()
)

display(conditions_silver)

conditions_silver.write.mode("overwrite").parquet(
    "/Volumes/workspace/default/ehr_raw/silver/conditions"
)

# --- Encounter aggregates ---
encounter_aggregates = (
    encounters_silver
    .groupBy("patient_id")
    .agg(
        F.count("*").alias("encounter_count"),
        F.min("start_ts").alias("first_encounter_ts"),
        F.max("start_ts").alias("last_encounter_ts"),
        F.avg("encounter_duration_minutes").alias("avg_encounter_duration_minutes"),
    )
)

# --- Condition aggregates ---
condition_counts = (
    conditions_silver
    .groupBy("patient_id")
    .agg(
        F.count("*").alias("condition_count")
    )
)

# --- Gold patient summary ---
patient_summary = (
    patients_silver.alias("p")
    .join(
        encounter_aggregates.alias("e"),
        on="patient_id",
        how="left"
    )
    .join(
        condition_counts.alias("c"),
        on="patient_id",
        how="left"
    )
    .withColumn(
        "encounter_count",
        F.coalesce(F.col("encounter_count"), F.lit(0))
    )
    .withColumn(
        "condition_count",
        F.coalesce(F.col("condition_count"), F.lit(0))
    )
)

display(patient_summary)

patient_summary.write.mode("overwrite").parquet(
    "/Volumes/workspace/default/ehr_raw/gold/patient_summary"
)

patient_summary_gold = spark.read.parquet(
    "/Volumes/workspace/default/ehr_raw/gold/patient_summary"
)

display(patient_summary_gold)
