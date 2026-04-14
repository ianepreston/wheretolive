# Databricks notebook source
# MAGIC %md
# MAGIC # Verify Schema Setup
# MAGIC Utility notebook to verify the Unity Catalog schema exists and is accessible.

catalog = "workspace"
schema = "wheretolive"

spark.sql(f"DESCRIBE SCHEMA {catalog}.{schema}").show()
print(f"Schema {catalog}.{schema} is accessible.")
