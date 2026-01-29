# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS slv.sap_persona (
# MAGIC
# MAGIC     ID             INT,
# MAGIC
# MAGIC     NOMBRE         STRING,
# MAGIC
# MAGIC     TELEFONO       STRING,
# MAGIC
# MAGIC     CORREO         STRING,
# MAGIC
# MAGIC     FECHA_INGRESO  DATE,
# MAGIC
# MAGIC     EDAD           INT,
# MAGIC
# MAGIC     SALARIO        DOUBLE,
# MAGIC
# MAGIC     ID_EMPRESA     INT,
# MAGIC
# MAGIC     EDAD_CATEGORIA STRING,
# MAGIC
# MAGIC     FECHA_PROCESO  DATE,
# MAGIC
# MAGIC     PERIODO        STRING,   -- 'yyyyMM' (por date_format)
# MAGIC
# MAGIC     ANIO           INT,
# MAGIC
# MAGIC     MES            INT,
# MAGIC
# MAGIC     DIA            INT,
# MAGIC
# MAGIC     DIA_TEXTO      STRING    -- nombre del día (EEEE)
# MAGIC
# MAGIC )
# MAGIC
# MAGIC USING DELTA
# MAGIC
# MAGIC PARTITIONED BY (ANIO, MES, DIA)