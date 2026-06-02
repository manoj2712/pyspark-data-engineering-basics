# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

employee_df = spark.read.format('csv')\
                    .option('header', 'true')\
                        .option('inferschema', 'true')\
                            .load('/Volumes/workspace/default/flight_data/employee_latest_data.csv')

# COMMAND ----------

employee_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC alias

# COMMAND ----------

employee_df.select(col('id').alias('employee_id'), 'name', 'address').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### filter/where 
# MAGIC both are same

# COMMAND ----------

employee_df.filter(col('salary')>75000).show()

# COMMAND ----------

employee_df.where(col('salary')>75000).show()

# COMMAND ----------

employee_df.filter(col('salary')>60000 & col('salary')<100000).show()

% we have to wrap col() in side paranthesis

# COMMAND ----------

employee_df.filter((col('salary')>60000) & (col('salary')<100000)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### literal
# MAGIC used to pass hardcode values to all the rows or we can say constant value.

# COMMAND ----------

employee_df.select("*", lit("lit_surname")).show()

# COMMAND ----------

employee_df.select("*", lit("lit_surname").alias('surname')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn() function
# MAGIC Adding new column or modifying using withColumn() function

# COMMAND ----------

employee_df.withColumn("surname", lit("patidar")).show()5

# COMMAND ----------

# MAGIC %md
# MAGIC ###Rename column
# MAGIC withColumnRenamed(col_name, new_col_name) function
# MAGIC and withColumnsRename(pass_dict_here_current_col_name_TO_new_col_name)

# COMMAND ----------

employee_df.withColumnRenamed("name", "first_name").show()

# COMMAND ----------

employee_df.withColumnsRenamed({'address':'current_address', 'id': 'employee_id'}).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Casting of column Data type

# COMMAND ----------

employee_df.withColumn("id", col('id').cast('string')).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### dataframes are immutable in spark. i cast id to string in last cell but in next cell it would be again integer. if we have to reflect changes than we have to store that in a new df

# COMMAND ----------

employee_df.printSchema()

# COMMAND ----------

employee_df.withColumn('id', col('id').cast('string'))\
                .withColumn('salary', col('salary').cast('double'))\
                    .printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove/drop Columns

# COMMAND ----------

employee_df.drop('address').show()

# COMMAND ----------

employee_df.drop('id', col('gender')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformation using Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC #### create a temp view or table

# COMMAND ----------

employee_df.createOrReplaceTempView('employee_tbl')

# COMMAND ----------

spark.sql("""
          select * from employee_tbl
          """).show()

# COMMAND ----------

spark.sql(
    """
    select * from employee_tbl where salary >75000
    """
).show()

# COMMAND ----------

spark.sql(
    """
    select * from employee_tbl where salary >75000 and gender = 'm'
    """
).show()

# COMMAND ----------

#literal in spark sql
spark.sql(
    """
    select *, 'patidar' as surname from employee_tbl
    """
).show()

# COMMAND ----------

#cast column
spark.sql(
    """
    select *, cast(salary as double) as salary_double from employee_tbl
    """
).printSchema()

# COMMAND ----------

