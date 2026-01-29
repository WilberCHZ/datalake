# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS SLV;

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS slv.sap_empresa")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS slv.sap_empresa (
# MAGIC     ID               INT,
# MAGIC     EMPRESA_NAME  STRING,
# MAGIC     fecha_proceso    DATE,
# MAGIC     periodo          STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (periodo)