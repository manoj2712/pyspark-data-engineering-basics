# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

employee_df = spark.read.format('csv')\
                .option('inferschema', 'true')\
                .option('header', 'true')\
                .load('/Volumes/workspace/default/flight_data/employee_latest_data.csv')

# COMMAND ----------

employee_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC string method to select columns 

# COMMAND ----------

employee_df.select("id", "name").show()

# COMMAND ----------

employee_df.select("id + 5").show()

# COMMAND ----------

# MAGIC %md
# MAGIC col method

# COMMAND ----------

employee_df.select(col('id'), col('address')).show()

# COMMAND ----------

employee_df.select("*").show()

# COMMAND ----------

employee_df.select(col('id') + 5).show()

# COMMAND ----------

# MAGIC %md
# MAGIC different select method in one expresssion

# COMMAND ----------

employee_df.select("id", col("name"), employee_df['age'], employee_df.address).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC expression

# COMMAND ----------

employee_df.select(expr('id')).show()

# COMMAND ----------

employee_df.select(expr('id + 5')).show()

# COMMAND ----------

employee_df.select(expr('id + 5').alias('new_id'), expr('name as employee_name')).show()

# COMMAND ----------

employee_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC sql method to select columns.
# MAGIC we have to first create a temp table or view to use sql commands

# COMMAND ----------

employee_df.createOrReplaceTempView("employee_tbl")

# COMMAND ----------

spark.sql("""
          select * from employee_tbl
          """).show()

# COMMAND ----------

