# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists brz;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Databricks notebook source
# MAGIC CREATE TABLE IF NOT EXISTS brz.sap_persona(
# MAGIC     ID STRING,
# MAGIC     NOMBRE STRING,
# MAGIC     TELEFONO STRING,
# MAGIC     CORREO STRING,
# MAGIC     FECHA_INGRESO STRING,
# MAGIC     EDAD STRING,
# MAGIC     SALARIO STRING,
# MAGIC     ID_EMPRESA STRING
# MAGIC ) 
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from brz.persona