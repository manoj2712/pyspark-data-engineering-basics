# PySpark Data Engineering Basics

## Overview

This repository contains hands-on PySpark and Databricks examples that I created while learning Apache Spark for Data Engineering.

The goal of this repository is to demonstrate practical Spark concepts including data ingestion, schema management, transformations, aggregations, joins, window functions, and data writing techniques.

## Technologies Used

* Apache Spark
* PySpark
* Databricks Community Edition
* Delta Lake
* Python

## Topics Covered

### Data Ingestion

* Reading CSV files
* Reading JSON files
* Reading Parquet files

### Schema Management

* Manual schema creation using StructType
* Nullable and non-nullable columns
* Schema validation

### Data Quality

* Handling corrupt records
* Using badRecordsPath
* Capturing malformed records

### Data Writing

* Writing DataFrames to disk
* CSV format
* Parquet format
* Delta format

### Performance Optimization

* Partitioning
* Bucketing concepts

### Data Transformations

* Select
* Filter
* withColumn
* withColumnRenamed
* when/otherwise
* Sorting
* Distinct records

### Combining Data

* Union
* UnionByName

### Aggregations

* Count
* Sum
* Average
* Min
* Max

### Grouping

* Group By operations
* Multiple aggregations

### Joins

* Inner Join
* Left Join
* Right Join
* Full Join
* Left Semi Join
* Left Anti Join

### Window Functions

* Row Number
* Rank
* Dense Rank
* Lead
* Lag

## Project Structure

The src folder contains topic-wise PySpark examples.

The notes folder contains personal Spark notes and interview preparation material.

## Learning Outcomes

Through this project I learned:

* Spark DataFrame API
* Schema enforcement
* Handling bad data
* Data transformations
* Window functions
* Spark optimization basics
* Databricks workflows

## Author

Manoj Patidar
