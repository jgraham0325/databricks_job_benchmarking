# Databricks notebook source
# DBTITLE 1,Task B: Join events with products
from pyspark.sql import functions as F

dbutils.widgets.text("input_schema", "", "Input schema (catalog.schema with bench_test_* tables)")
dbutils.widgets.text("output_schema", "", "Output schema (catalog.schema)")
input_schema = dbutils.widgets.get("input_schema").strip()
output_schema = dbutils.widgets.get("output_schema").strip()
assert input_schema and output_schema, "input_schema and output_schema are required"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}")

events = spark.table(f"{input_schema}.bench_test_events")
products = spark.table(f"{input_schema}.bench_test_products")
result = (
    events
    .join(products, "product_id", "inner")
    .withColumn("profit", F.col("amount") - F.col("unit_cost") * F.col("quantity"))
    .select("event_date", "product_id", "product_name", "category",
            "region", "quantity", "amount", "unit_cost", "profit")
)
result.write.partitionBy("event_date").mode("overwrite").saveAsTable(f"{output_schema}.test_output_enriched")
print(f"Wrote {spark.table(f'{output_schema}.test_output_enriched').count():,} rows")
