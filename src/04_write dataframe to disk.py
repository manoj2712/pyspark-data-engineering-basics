# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

df_employee = spark.read.format('csv')\
                    .option("header","true")\
                        .option("inferschema","true")\
                            .load('/Volumes/workspace/default/flight_data/employee_latest_data.csv')

# COMMAND ----------

df_employee.show()

# COMMAND ----------

df_employee.write.format('csv')\
    .option('header','true')\
        .mode('overwrite')\
            .option('path', '/Volumes/workspace/default/flight_data/employee_latest_transformed_data/')\
                .save()

# COMMAND ----------

dbutils.fs.ls('/Volumes/workspace/default/flight_data/employee_latest_transformed_data/')

# COMMAND ----------

df_employee.repartition(3).write.format('csv')\
    .option('header','true')\
        .mode('overwrite')\
            .option('path', '/Volumes/workspace/default/flight_data/employee_latest_transformed_data/')\
                .save()

# COMMAND ----------

