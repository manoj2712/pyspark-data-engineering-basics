# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

emp_data =[(1,'manish',50000,'IT'),
(2,'vikash',60000,'sales'),
(3,'raushan',70000,'marketing'),
(4,'mukesh',80000,'IT'),
(5,'pritam',90000,'sales'),
(6,'nikita',45000,'marketing'),
(7,'ragini',55000,'marketing'),
(8,'rakesh',100000,'IT'),
(9,'aditya',65000,'IT'),
(10,'rahul',50000,'marketing')]


# COMMAND ----------

emp_data_with_contry =[(1,'manish',50000,'IT', 'india'),
(2,'vikash',60000,'sales','us'),
(3,'raushan',70000,'marketing','india'),
(4,'mukesh',80000,'IT','us'),
(5,'pritam',90000,'sales','india'),
(6,'nikita',45000,'marketing','us'),
(7,'ragini',55000,'marketing','india'),
(8,'rakesh',100000,'IT','us'),
(9,'aditya',65000,'IT','india'),
(10,'rahul',50000,'marketing','us')]

emp_df_with_contry = spark.createDataFrame(emp_data_with_contry,['id','name','salary','department','country'])

# COMMAND ----------

emp_df = spark.createDataFrame(emp_data,['id','name','salary','department'])

# COMMAND ----------

emp_df.show()

# COMMAND ----------

emp_df.groupBy('department')\
            .agg(sum('salary').alias(' sum of salary dept wise')).show()

# COMMAND ----------

emp_df_with_contry.groupBy('department','country')\
            .agg(sum('salary').alias(' sum of salary dept-country wise')).show()

# COMMAND ----------

emp_df_with_contry.createOrReplaceTempView('emp_tbl_with_country')

# COMMAND ----------

spark.sql(
    """
    select department, country, sum(salary) from emp_tbl_with_country group by department, country
    """
).show()

# COMMAND ----------

