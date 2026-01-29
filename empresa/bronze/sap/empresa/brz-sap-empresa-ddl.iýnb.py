# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists brz;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Databricks notebook source
# MAGIC CREATE TABLE IF NOT EXISTS brz.sap_empresa(
# MAGIC     ID STRING,
# MAGIC     EMPRESA_NAME STRING
# MAGIC ) 
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from brz.sap_empresa