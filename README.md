# DataEngineeringProjects

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their analytics team is particularly interested in understanding what songs users are listening to. 

## Data Warehousing with Amazon Redshift
 **Problem Statement:**
 
Sparkify user base and song database has grown and want to move their processes and data onto the cloud. 

Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. ETL pipeline has to be built that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to finding insights.

Staging tables, facts and dimension tables are to be defined in star schema using Amazon Redshift.

ETL pipelines that transfers data from files in json format to Amazon Redshift database are to be developed using python.

[Go to project](https://github.com/RangaAmirapu/DataEngineeringProjects/tree/master/DataWarehousingWithAmazonRedshift)

## Data Modelling with Postgres
 **Problem Statement:**
 
 The goal of this project is to develop a data model and ETL process for song play analysis

Data modelling is to be done based on the raw data available in json format. Facts and dimension tables are to be defined in star schema using Postgres database

ETL pipelines that transfers data from files in json format to Postgres database are to be developed using python

 [Go to project](https://github.com/RangaAmirapu/DataEngineeringProjects/tree/master/DataModelingWithPostgres)

## Data Modelling with Apache Cassandra

 **Problem Statement:**
 
Currently, for Sparkify there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

The goal of this project is to create an Apache Cassandra database which can create queries on song play data analysis.

Data modelling is to be done based on the raw data available in CSV format.

ETL pipelines that transfers data from CSV files to Cassandra database are to be developed using python.

[Go to project](https://github.com/RangaAmirapu/DataEngineeringProjects/tree/master/DataModellingWithCassandra)