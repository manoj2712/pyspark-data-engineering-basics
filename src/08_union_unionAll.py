# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###UNION
# MAGIC
# MAGIC
# MAGIC ####combines results
# MAGIC #### removes duplicate rows

# COMMAND ----------

# MAGIC %md
# MAGIC ###UNION ALL:
# MAGIC
# MAGIC ####combines results
# MAGIC ####keeps duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC #####Technology	                       union	                   unionAll
# MAGIC #####PySpark DataFrame API	    keeps duplicates	            keeps duplicates
# MAGIC #####SQL / Spark SQL	            removes duplicates	            keeps duplicates

# COMMAND ----------

data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17)]

data1=[(19 ,'Sohan',50000, 18),
(20 ,'Sima',75000,  17)]

data_duplicate=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18)]

wrong_column_data=[(19 ,50000, 18,'Sohan'),
(20 ,75000,  17,'Sima')]


wrong_column_data_1=[(19 ,50000, 18,'Sohan',10),
(20 ,75000,  17,'Sima',20)]

# COMMAND ----------

manager_df = spark.createDataFrame(data, schema=["id", "name", "salary", "age"])

# COMMAND ----------

manager_df.show()

# COMMAND ----------

manager_df.count()

# COMMAND ----------

schema_df1 = ["id", "name", "salary", "age"]
manager_df1 = spark.createDataFrame(data1, schema_df1)

# COMMAND ----------

manager_df1.show()

# COMMAND ----------

manager_df1.count()

# COMMAND ----------

manager_df.union(manager_df1).show()

# COMMAND ----------

manager_df.union(manager_df1).count()

# COMMAND ----------

manager_df.unionAll(manager_df1).count()

# COMMAND ----------

schema_df2 = ["id", "name", "salary", "age"]
manager_df_duplicate = spark.createDataFrame(data_duplicate, schema_df2)
manager_df_duplicate.show()
manager_df_duplicate.count()

# COMMAND ----------

manager_df.union(manager_df_duplicate).count()

# COMMAND ----------

manager_df.unionAll(manager_df_duplicate).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Note
# MAGIC  both have same records. duplicates were not removed in union All

# COMMAND ----------

manager_df.createOrReplaceTempView('manager_df_tbl')
manager_df_duplicate.createOrReplaceTempView('manager_df_duplicate_tbl')

# COMMAND ----------

spark.sql(
    """
    select * from manager_df_tbl 
    union 
    select * from manager_df_duplicate_tbl
    """
).count()

# COMMAND ----------

spark.sql(
    """
    select * from manager_df_tbl
    union all
    select * from manager_df_duplicate_tbl
    """
).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Note
# MAGIC ####number of column should be same in both the dataframe/table
# MAGIC ####sequence of column should be same in both the dataframe/table. spark will not check for order. it will just add them as it is.
# MAGIC ####to overcome above problem we use unionByName() 
# MAGIC

# COMMAND ----------

df_manager_wrong = spark.createDataFrame(wrong_column_data, ["age", "name","id", "salary"])
df_manager_wrong.show()
df_manager_wrong.count()
df_manager_wrong.createOrReplaceTempView('df_manager_wrong_tbl')

# COMMAND ----------

manager_df.show()

# COMMAND ----------

manager_df.union(df_manager_wrong).show()

# COMMAND ----------

