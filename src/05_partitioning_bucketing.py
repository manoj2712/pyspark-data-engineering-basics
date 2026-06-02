# Databricks notebook source
df_employee = spark.read.format('csv')\
                    .option("header","true")\
                        .option("inferschema","true")\
                            .load('/Volumes/workspace/default/flight_data/employee_latest_data.csv')

# COMMAND ----------

df_employee.printSchema()

# COMMAND ----------

df_employee.show()

# COMMAND ----------

df_employee.write.format('csv')\
                .option("header","true")\
                    .mode("overwrite")\
                        .partitionBy("gender")\
                            .option("path","/Volumes/workspace/default/flight_data/employee_latest_data_partitioned_gender/")\
                                .save()

# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/default/flight_data/employee_latest_data_partitioned_gender/")

# COMMAND ----------

df_employee.write.format('csv')\
                .option("header","true")\
                    .mode("overwrite")\
                        .partitionBy("address")\
                            .option("path","/Volumes/workspace/default/flight_data/employee_latest_data_partitioned_address/")\
                                .save()


# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/default/flight_data/employee_latest_data_partitioned_address/")

# COMMAND ----------

df_employee.write.format('csv')\
                .option("header","true")\
                    .mode("overwrite")\
                        .partitionBy("address","gender")\
                            .option("path","/Volumes/workspace/default/flight_data/employee_latest_data_partitioned_address_gender/")\
                                .save()


# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/default/flight_data/employee_latest_data_partitioned_address_gender/")

# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/default/flight_data/employee_latest_data_partitioned_address_gender/address=INDIA/")

# COMMAND ----------

df_employee.write.format('csv')\
                .option("header","true")\
                    .mode("overwrite")\
                        .partitionBy("gender", "address")\
                            .option("path","/Volumes/workspace/default/flight_data/employee_latest_data_partitioned_gender_address/")\
                                .save()


# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/default/flight_data/employee_latest_data_partitioned_gender_address/")

# COMMAND ----------

dbutils.fs.ls("/Volumes/workspace/default/flight_data/employee_latest_data_partitioned_gender_address/gender=f/")

# COMMAND ----------

df_employee.write.format('delta')\
                .option("header","true")\
                    .mode("overwrite")\
                        .bucketBy(3,"id")\
                                .saveAsTable("workspace.default.employee_data_bucketed_by_id")

# COMMAND ----------

