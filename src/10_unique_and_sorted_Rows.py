# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17),
(15 ,'Mohit',45000,  18),
(13 ,'Nidhi',60000,  17),      
(14 ,'Priya',90000,  18),  
(18 ,'Sam',65000,   17)
     ]

     
leet_code_data = [
    (1, 'Will', None),
    (2, 'Jane', None),
    (3, 'Alex', 2),
    (4, 'Bill', None),
    (5, 'Zack', 1),
    (6, 'Mark', 2)
]

# COMMAND ----------

manager_df = spark.createDataFrame(data, ['id', 'name','salary', 'mgr_id'])
manager_df.show()

# COMMAND ----------

manager_df.createOrReplaceTempView('manager_df')

# COMMAND ----------

manager_df.count()

# COMMAND ----------

manager_df.distinct().count()

# COMMAND ----------

manager_df.select('id','name').distinct().count()

# COMMAND ----------

manager_df.select("name").distinct().count()

# COMMAND ----------

manager_df.dropDuplicates().show()

# COMMAND ----------

manager_df.dropDuplicates(['id','name']).show()

# COMMAND ----------

manager_df.sort('salary').show()

# COMMAND ----------

manager_df.sort(col('salary')).show()

# COMMAND ----------

manager_df.sort(col('salary').desc()).show()

# COMMAND ----------

manager_df.sort(col('salary').desc(), col('name')).show()

# COMMAND ----------

