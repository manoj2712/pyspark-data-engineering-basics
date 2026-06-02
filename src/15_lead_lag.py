# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

product_data = [
(1,"iphone","01-01-2023",1500000),
(2,"samsung","01-01-2023",1100000),
(3,"oneplus","01-01-2023",1100000),
(1,"iphone","01-02-2023",1300000),
(2,"samsung","01-02-2023",1120000),
(3,"oneplus","01-02-2023",1120000),
(1,"iphone","01-03-2023",1600000),
(2,"samsung","01-03-2023",1080000),
(3,"oneplus","01-03-2023",1160000),
(1,"iphone","01-04-2023",1700000),
(2,"samsung","01-04-2023",1800000),
(3,"oneplus","01-04-2023",1170000),
(1,"iphone","01-05-2023",1200000),
(2,"samsung","01-05-2023",980000),
(3,"oneplus","01-05-2023",1175000),
(1,"iphone","01-06-2023",1100000),
(2,"samsung","01-06-2023",1100000),
(3,"oneplus","01-06-2023",1200000)
]

# COMMAND ----------

product_df = spark.createDataFrame(data=product_data,schema=['id','product','sales_date','price'])
product_df.show()

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

emp_schema=['id','name','salary','dept','gender']

emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)
emp_df = emp_df.select('id','name','salary','gender','dept')
emp_df.show()

# COMMAND ----------

window = Window.partitionBy(col('id')).orderBy(col('sales_date'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##lag function syntax
# MAGIC ### lag(col_name, jump_row_by, default value of null(optional))

# COMMAND ----------

product_df.withColumn("previous_date_sales", lag(col('price'),1).over(window)).show()

# COMMAND ----------

product_df.withColumn("previous_date_sales", lag(col('price'),1, 100).over(window)).show()

# COMMAND ----------

product_df.withColumn("previous_date_sales", lag(col('price'),2).over(window)).show()

# COMMAND ----------

product_df.withColumn("previous_date_sales", lead(col('price'),1).over(window)).show()

# COMMAND ----------

last_month_df = product_df.withColumn("previous_month_sales", lag(col('price'),1).over(window))

# COMMAND ----------

last_month_df.withColumn('last_month_profit_loss', 
                         (
                             (col('price')-col("previous_month_sales"))
                            /col('price')
                            )*100)\
                         .show()

# COMMAND ----------

last_month_df.withColumn('last_month_profit_loss', 
                        round((
                             (col('price')-col("previous_month_sales"))
                            /col('price')
                            )*100,2))\
                         .show()

# COMMAND ----------

