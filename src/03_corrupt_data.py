# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

my_schema = StructType([
    StructField('id',IntegerType(),True),
    StructField('name',StringType(),True),
    StructField('age',IntegerType(),True),
    StructField('salary',IntegerType(),True),
    StructField('address',StringType(),True),
    StructField('nomiee',StringType(),True),
    StructField('_corrupt_record',StringType(),True)
])

# COMMAND ----------

df_employee = spark.read.format('csv')\
                    .option('header',True)\
                        .option('inferSchema',False)\
                            .schema(my_schema)\
                                .load('/Volumes/workspace/default/flight_data/corrupt_employee_records.csv')

# COMMAND ----------

df_employee.show()

# COMMAND ----------

df_employee.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Stroring corrupt records details

# COMMAND ----------

df_employee = spark.read.format('csv')\
                    .option('header',True)\
                        .option('inferSchema',False)\
                            .schema(my_schema)\
                                .option('badRecordsPath','/Volumes/workspace/default/flight_data/bad_records/')\
                                .load('/Volumes/workspace/default/flight_data/corrupt_employee_records.csv')

# COMMAND ----------

df_employee.show()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/workspace/default/flight_data

# COMMAND ----------

