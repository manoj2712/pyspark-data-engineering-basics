# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

 emp_data = [
(1,'manish',26,20000,'india','IT'),
(2,'rahul',None,40000,'germany','engineering'),
(3,'pawan',12,60000,'india','sales'),
(4,'roshini',44,None,'uk','engineering'),
(5,'raushan',35,70000,'india','sales'),
(6,None,29,200000,'uk','IT'),
(7,'adam',37,65000,'us','IT'),
(8,'chris',16,40000,'us','sales'),
(None,None,None,None,None,None),
(7,'adam',37,65000,'us','IT')
]
 
emp_schema = ['id', 'name', 'age', 'salary', 'country', 'department']
emp_df = spark.createDataFrame(emp_data, emp_schema)

emp_df.createOrReplaceTempView("emp_tbl")

# COMMAND ----------

emp_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **count is both an action and transformation**

# COMMAND ----------

emp_df.count() # this is an action

# COMMAND ----------

emp_df.select(count('id')).show() # this is an transformation
# here it ignored the row where id is null.

# COMMAND ----------

emp_df.select(count("name")).show() # we have two rows where name is NULL

# COMMAND ----------

emp_df.select(sum('salary'), min('salary'), max('salary')).show()

# COMMAND ----------

emp_df.select(sum('salary').alias('total salary'), min('salary').alias('min salary'), max('salary').alias('max salary'), avg('salary').alias('avg salary')).show()

# COMMAND ----------

emp_df.select(avg('salary')).show()

# COMMAND ----------

emp_df.select((sum('salary')/count('salary')).alias('avg salary')).show()

# COMMAND ----------

