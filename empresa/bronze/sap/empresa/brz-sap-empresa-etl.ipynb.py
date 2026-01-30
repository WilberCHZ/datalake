# Databricks notebook source
# Databricks notebook source
# Este es un cambio
# Este es otro cambio
# Este es un cambio mas
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

bucket = "/Volumes"

landing_layer = "/workspace/datalake/landing" 

bronze_layer = "/bronze" 

system_source = "/sap" 

table = "/empresas"

file_name = "/empresa.data"

db_target = 'brz'

table_name = 'sap_empresa'

path_source = f'{bucket}{landing_layer}{system_source}{table}{file_name}'

table_target = f'{db_target}.{table_name}'
#table_target='/Volumes/workspace/datalake/landing/sap/empresas/empresa.data'
 

print(path_source)

print(table_target)

# COMMAND ----------

df_schema = StructType([
StructField("ID", StringType(),True),
StructField("EMPRESA_NAME", StringType(),True)
])



# COMMAND ----------

# COMMAND ----------

df = spark.read.format("CSV").option("header","true").option("delimiter","|").schema(df_schema).load(path_source)



# COMMAND ----------


display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable(table_target)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from brz.sap_empresa