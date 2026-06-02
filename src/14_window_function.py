# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

emp_data = [(1,'manish',50000,'IT','m'),
(2,'vikash',60000,'sales','m'),
(3,'raushan',70000,'marketing','m'),
(4,'mukesh',80000,'IT','m'),
(5,'priti',90000,'sales','f'),
(6,'nikita',45000,'marketing','f'),
(7,'ragini',55000,'marketing','f'),
(8,'rashi',100000,'IT','f'),
(9,'aditya',65000,'IT','m'),
(10,'rahul',50000,'marketing','m'),
(11,'rakhi',50000,'IT','f'),
(12,'akhilesh',90000,'sales','m')]

emp_schema = ['id', 'name', 'salary', 'dept', 'gender']

emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)
emp_df.show()


# COMMAND ----------

emp_df.groupBy('dept').agg(sum('salary')).show()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

window = Window.partitionBy('dept').orderBy('salary') # orderBy is optional

# COMMAND ----------

emp_df.withColumn('rank', rank().over(window)).show()

# COMMAND ----------

emp_df.withColumn('row',row_number().over(window)).show()

# COMMAND ----------

emp_df.withColumn('dense rank', dense_rank().over(window)).show()

# COMMAND ----------

emp_df.withColumn('row', row_number().over(window))\
            .withColumn('rank', rank().over(window))\
            .withColumn('dense rank', dense_rank().over(window))\
            .withColumn('percent rank', percent_rank().over(window))\
            .show()

# COMMAND ----------

