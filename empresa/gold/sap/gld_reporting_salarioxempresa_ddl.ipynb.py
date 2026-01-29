# Databricks notebook source

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df_empresa = spark.table("slv.sap_empresa")
df_persona = spark.table("slv.sap_persona")

# COMMAND ----------

df_persona.createOrReplaceTempView("persona")
df_empresa.createOrReplaceTempView("empresa")

# COMMAND ----------

display(df_persona)

# COMMAND ----------

display(df_empresa)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

query = """
    select e.empresa_name as nombre,count(1) as cantidad_empleados, avg(p.edad) as avg_edad, avg(p.salario) as avg_salario, max(salario) as max_salario,  min(salario) as min_salario,
    sum(salario) as planilla  from persona p 
    inner join empresa e on p.id_empresa = e.id 
    group by e.empresa_name
    """
df_result = spark.sql(query)
display(df_result)
df_result.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema gld

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gld.resumen_empresas;
# MAGIC
# MAGIC CREATE TABLE gld.resumen_empresas (
# MAGIC     nombre             STRING,
# MAGIC     cantidad_empleados BIGINT      NOT NULL,
# MAGIC     avg_edad           DOUBLE,
# MAGIC     avg_salario        DOUBLE,
# MAGIC     max_salario        DOUBLE,
# MAGIC     min_salario        DOUBLE,
# MAGIC     planilla           DOUBLE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

df_result.write.mode("overwrite").format("delta").saveAsTable("gld.resumen_empresas")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gld.resumen_empresas