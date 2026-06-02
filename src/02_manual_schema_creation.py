# Databricks notebook source
# MAGIC %md
# MAGIC ##2 ways to create manual schema: 
# MAGIC ###i. StructType and StructField
# MAGIC ###ii. DDL

# COMMAND ----------

# MAGIC %md
# MAGIC ##StructType and StructField

# COMMAND ----------

# MAGIC %md
# MAGIC import library to use StructType and StructField

# COMMAND ----------

from pyspark.sql.types import *


# COMMAND ----------

my_schema = StructType([
    StructField('country name', StringType(), True),
    StructField('origin', StringType(), True),
    StructField('count', IntegerType(), False)
])

# COMMAND ----------

df_flight = spark.read.format("csv")\
                .option("header","true")\
                .schema(my_schema)\
                .load("/Volumes/workspace/default/flight_data/flight_data.csv")


# COMMAND ----------

display(df_flight)

# COMMAND ----------

df_flight.printSchema()

# COMMAND ----------

