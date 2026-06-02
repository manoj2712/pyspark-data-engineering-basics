# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

customer_data = [(1,'manish','patna',"30-05-2022"),
(2,'vikash','kolkata',"12-03-2023"),
(3,'nikita','delhi',"25-06-2023"),
(4,'rahul','ranchi',"24-03-2023"),
(5,'mahesh','jaipur',"22-03-2023"),
(6,'prantosh','kolkata',"18-10-2022"),
(7,'raman','patna',"30-12-2022"),
(8,'prakash','ranchi',"24-02-2023"),
(9,'ragini','kolkata',"03-03-2023"),
(10,'raushan','jaipur',"05-02-2023")]

customer_schema=['customer_id','customer_name','address','date_of_joining']


sales_data = [(1,22,10,"01-06-2022"),
(1,27,5,"03-02-2023"),
(2,5,3,"01-06-2023"),
(5,22,1,"22-03-2023"),
(7,22,4,"03-02-2023"),
(9,5,6,"03-03-2023"),
(2,1,12,"15-06-2023"),
(1,56,2,"25-06-2023"),
(5,12,5,"15-04-2023"),
(11,12,76,"12-03-2023")]

sales_schema=['customer_id','product_id','quantity','date_of_purchase']


product_data = [(1, 'fanta',20),
(2, 'dew',22),
(5, 'sprite',40),
(7, 'redbull',100),
(12,'mazza',45),
(22,'coke',27),
(25,'limca',21),
(27,'pepsi',14),
(56,'sting',10)]

product_schema=['id','name','price']



# COMMAND ----------

customer_df = spark.createDataFrame(customer_data, customer_schema)
sales_df = spark.createDataFrame(sales_data, sales_schema)
product_df = spark.createDataFrame(product_data, product_schema)

# COMMAND ----------

print("customer df")
customer_df.show()
print("sales df")
sales_df.show()
print("product df")
product_df.show()

# COMMAND ----------

customer_df.join(sales_df, customer_df['customer_id']== sales_df['customer_id'], "inner").show()

# COMMAND ----------

sales_df.join(customer_df, sales_df['customer_id'] == customer_df['customer_id'], "inner").show()

# COMMAND ----------

customer_df.join(sales_df, customer_df['customer_id'] == sales_df['customer_id'], "inner").select("customer_id").show()

# COMMAND ----------

customer_df.join(sales_df, customer_df['customer_id'] == sales_df['customer_id'], "inner").select(customer_df["customer_id"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###left join

# COMMAND ----------

customer_df.join(sales_df, customer_df['customer_id'] == sales_df['customer_id'], 'left').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Right join

# COMMAND ----------

sales_df.join(customer_df, customer_df['customer_id'] == sales_df['customer_id'], 'right').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### full outer join / outer join

# COMMAND ----------

customer_df.join(sales_df, customer_df['customer_id'] == sales_df['customer_id'], 'outer').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Left semi join
# MAGIC ####give left table records only where we have an entry on right table for that

# COMMAND ----------

customer_df.join(sales_df, customer_df['customer_id']== sales_df['customer_id'], 'left_semi').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####left anti join
# MAGIC #### record which are in left table but not in righ table.

# COMMAND ----------

customer_df.join(sales_df, customer_df['customer_id'] == sales_df['customer_id'], 'left_anti').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###cross join

# COMMAND ----------

customer_df.crossJoin(sales_df).show()

# COMMAND ----------

customer_df.crossJoin(sales_df).count()

# COMMAND ----------

customer_df.crossJoin(sales_df).explain()

# COMMAND ----------

