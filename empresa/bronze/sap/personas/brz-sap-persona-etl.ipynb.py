# Databricks notebook source
# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

bucket = "/Volumes"

landing_layer = "/workspace/datalake/landing" 

bronze_layer = "/bronze" 

system_source = "/sap" 

table = "/personas"

file_name = "/persona.data"

db_target = 'brz'

table_name = 'sap_persona'

path_source = f'{bucket}{landing_layer}{system_source}{table}{file_name}'

table_target = f'{db_target}.{table_name}'

 

print(path_source)

print(table_target)

# COMMAND ----------

df_schema = StructType([
StructField("ID", StringType(),True),
StructField("NOMBRE", StringType(),True),
StructField("TELEFONO", StringType(),True),
StructField("CORREO", StringType(),True),
StructField("FECHA_INGRESO", StringType(),True),
StructField("EDAD", StringType(),True),
StructField("SALARIO", StringType(),True),
StructField("ID_EMPRESA", StringType(),True),
])



# COMMAND ----------

# COMMAND ----------

df = spark.read.format("CSV").option("header","true").option("delimiter","|").schema(df_schema).load(path_source)



# COMMAND ----------


#display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable(table_target)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from brz.sap_persona