# Udacity Data Engineering Projects

This repository consists of several projects related to data engineering including Data Modeling, Data Warehousing, Data Lake development, AWS cloud infrastructure setup, and Data Pipeline Management.

## 1. Data Modeling

### Data Modeling with PostgreSQL
In this project, we put into practice the following concepts, data modeling with Postgres, database star schema design, ETL pipelining. A startup wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Currently, they are collecting data in json format and the analytics team is particularly interested in understanding what songs users are listening to.

### Data Modeling with Cassandra
In this project, we apply Data Modeling with Cassandra and build an ETL pipeline using Python. We will build a Data Model around our queries that we want to get answers for.

## 2. Data Warehouse
In this project, we apply the Data Warehouse architectures we learnt and build a Data Warehouse on AWS cloud. We build an ETL pipeline to extract and transform data stored in json format in s3 buckets and move the data to Warehouse hosted on Amazon Redshift.

## 3. Data Lake
In this project, we will build a Data Lake on AWS cloud using Spark and AWS EMR cluster. The data lake will serve as a Single Source of Truth for the Analytics Platform. We will write spark jobs to perform ELT operations that picks data from landing zone on S3 and transform and stores data on the S3 processed zone.

## 4. Data Pipelines with Airflow
In this project, we will orchestrate our Data Pipeline workflow using an open-source Apache project called Apache Airflow. We will schedule our ETL jobs in Airflow, create project related custom plugins and operators and automate the pipeline execution.

