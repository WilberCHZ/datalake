# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,IntegerType,DoubleType
from pyspark.sql.functions import regexp_replace,col,to_date,when,current_date,date_format,dayofmonth,year,month


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

db_source= 'brz' # Capa bronze
db_target = 'slv' # Capa silver
table_name = 'sap_empresa'
table_source = f'{db_source}.{table_name}'
table_target = f'{db_target}.{table_name}'

 

print(table_source)

print(table_target)

# COMMAND ----------

df  = spark.table(table_source)


# COMMAND ----------

display(df) 

# COMMAND ----------

df_t = df.withColumn('ID',col('ID').cast(IntegerType()))\
    .withColumn('fecha_proceso', current_date())\
    .withColumn('periodo', date_format(current_date(),'yyyyMM'))

# COMMAND ----------

display(df_t) 

# COMMAND ----------

#df_t.write.option("mergeSchema", "true").mode("overwrite").format("delta").partitionBy('periodo').saveAsTable(table_target)
df_t.write.mode("overwrite").format("delta").partitionBy('periodo').saveAsTable(table_target)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from slv.sap_empresa