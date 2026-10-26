# Databricks notebook source
# DBTITLE 1,Task C: Aggregate daily summary
from pyspark.sql import functions as F

dbutils.widgets.text("input_schema", "", "Input schema (catalog.schema with bench_test_* tables)")
dbutils.widgets.text("output_schema", "", "Output schema (catalog.schema)")
input_schema = dbutils.widgets.get("input_schema").strip()
output_schema = dbutils.widgets.get("output_schema").strip()
assert input_schema and output_schema, "input_schema and output_schema are required"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}")

events = spark.table(f"{input_schema}.bench_test_events")
result = (
    events
    .groupBy("event_date", "region")
    .agg(
        F.count("*").alias("num_events"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("product_id").alias("distinct_products"),
    )
)
result.write.partitionBy("event_date").mode("overwrite").saveAsTable(f"{output_schema}.test_output_summary")
print(f"Wrote {spark.table(f'{output_schema}.test_output_summary').count():,} rows")
