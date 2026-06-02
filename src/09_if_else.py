# Databricks notebook source
# MAGIC %md
# MAGIC we use when otherwise in pyspark for if else

# COMMAND ----------

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

# COMMAND ----------

emp_schema = ['id', 'name', 'age', 'salary', 'coutry', 'department']

# COMMAND ----------

employee_df = spark.createDataFrame(emp_data, emp_schema)
employee_df.show()

# COMMAND ----------

employee_df.withColumn('isAdult', when(col('age')<18, "No")\
                                    .when(col('age')>=18, "Yes")\
                                        .otherwise('Unknown') 
                ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC fixing null values for age

# COMMAND ----------

employee_df.withColumn('age', when(col('age').isNull(),19)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC whenever we are passing when then we have to use otherwise, else the other records which didn't satisfy the condition gets value updated to NULL

# COMMAND ----------

employee_df.withColumn('age', when(col('age').isNull(), lit(19))\
                            .otherwise(col('age'))).show()

# COMMAND ----------

employee_df.withColumn('age', when(col('age').isNull(), lit(19))\
                                .otherwise(col('age')))\
                                    .withColumn('isAdult', when(col('age')<18, "No")\
                                    .when(col('age')>=18,"Yes")\
                                        .otherwise('Unknown')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **if else using spark sql**

# COMMAND ----------

employee_df.createOrReplaceTempView('employee_tbl')

# COMMAND ----------

spark.sql(
    """
    select * from employee_tbl
    """
).show()

# COMMAND ----------

spark.sql(
    """
    select *, case when age < 18 then 'No' when age >= 18 then 'Yes' else 'Unknown' end as isAdult from employee_tbl
    """
).show()

# COMMAND ----------

