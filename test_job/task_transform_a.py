# Databricks notebook source
# DBTITLE 1,Task A: Filter and transform events
from pyspark.sql import functions as F

dbutils.widgets.text("input_schema", "", "Input schema (catalog.schema with bench_test_* tables)")
dbutils.widgets.text("output_schema", "", "Output schema (catalog.schema)")
input_schema = dbutils.widgets.get("input_schema").strip()
output_schema = dbutils.widgets.get("output_schema").strip()
assert input_schema and output_schema, "input_schema and output_schema are required"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}")

events = spark.table(f"{input_schema}.bench_test_events")
# Last 14 days of data, relative to the newest event so every run reads the same rows
max_date = events.agg(F.max("event_date")).first()[0]
result = (
    events
    .filter(F.col("event_date") >= F.date_sub(F.lit(max_date), 14))
    .withColumn("total_value", F.col("quantity") * F.col("amount"))
    .withColumn("value_band", F.when(F.col("amount") > 75, "high")
                                .when(F.col("amount") > 25, "medium")
                                .otherwise("low"))
)
result.write.partitionBy("event_date").mode("overwrite").saveAsTable(f"{output_schema}.test_output_events")
print(f"Wrote {spark.table(f'{output_schema}.test_output_events').count():,} rows")
