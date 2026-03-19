"""
Validation Layer: Run data quality checks on silver EHR tables and persist
validation results for downstream review.

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

# --- Referential integrity validation ---
# Does every encounter reference a patient that actually exists in patients_silver?
orphan_encounters = (
    encounters_silver.alias("e")
    .join(
        patients_silver.alias("p"),
        on="patient_id",
        how="left"
    )
    .filter(F.col("p.patient_id").isNull())
)

orphan_encounter_count = orphan_encounters.count()

# --- Temporal validation ---
# Check for encounters where end time is earlier than start time.
invalid_encounters = (
    encounters_silver
    .filter(F.col("end_ts") < F.col("start_ts"))
)

invalid_encounter_count = invalid_encounters.count()

validation_results = spark.createDataFrame(
    [
        ("encounters", "orphan_patient_fk", orphan_encounter_count),
        ("encounters", "invalid_time_range", invalid_encounter_count),
    ],
    ["table_name", "check_name", "failed_rows"]
)

display(validation_results)

validation_results.write.mode("overwrite").parquet(
    "/Volumes/workspace/default/ehr_raw/outputs/validation_results"
)
