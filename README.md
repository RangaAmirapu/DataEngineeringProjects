# Data Engineering Projects

Data engineering projects on data modelling, data warehousing, data lake development, orchestration and analysis.

 - [Building a Data Lake using Amazon S3 and EMR](https://github.com/RangaAmirapu/DataEngineeringProjects/tree/master/DataLakesWithAmazonS3andEMR)
 - [Building a Data Warehouse using AWS Redshift](https://github.com/RangaAmirapu/DataEngineeringProjects/tree/master/DataWarehousingWithAmazonRedshift)
 - [Data Modelling with Postgres](https://github.com/RangaAmirapu/DataEngineeringProjects/tree/master/DataModelingWithPostgres)
 - [Data Modelling with Cassandra](https://github.com/RangaAmirapu/DataEngineeringProjects/tree/master/DataModellingWithCassandra)
 
## Building a Data Lake using Amazon S3 and EMR

 **Problem Statement:**
 
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their user base and song database have grown large and want to move their data warehouse to a data lake.

Sparkify user base and song database has grown and want to move their processes and data onto the cloud. 

The goal of this project is to develop a data lake and ETL process for song play analysis.

Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. ETL pipeline has to be built that extracts data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables in parquet files. This will allow their analytics team to continue finding insights in what songs their users are listening to.

ETL pipelines that transfers data from files in json format to Amazon S3 are to be developed using python and deploy on a cluster built using Amazon EMR.

[**Go to project**](https://github.com/RangaAmirapu/DataEngineeringProjects/tree/master/DataLakesWithAmazonS3andEMR)

## Building a Data Warehousing with Amazon Redshift 

 **Problem Statement:**
 
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their analytics team is particularly interested in understanding what songs users are listening to. 

Sparkify user base and song database has grown and want to move their processes and data onto the cloud. 

Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. ETL pipeline has to be built that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to finding insights.

Staging tables, facts and dimension tables are to be defined in star schema using Amazon Redshift.

ETL pipelines that transfers data from files in json format to Amazon Redshift database are to be developed using python.

[**Go to project**](https://github.com/RangaAmirapu/DataEngineeringProjects/tree/master/DataWarehousingWithAmazonRedshift)

## Data Modelling with Postgres
 **Problem Statement:**
 
 A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their analytics team is particularly interested in understanding what songs users are listening to. 
 
The goal of this project is to develop a data model and ETL process for song play analysis

Data modelling is to be done based on the raw data available in json format. Facts and dimension tables are to be defined in star schema using Postgres database

ETL pipelines that transfers data from files in json format to Postgres database are to be developed using python

 [**Go to project**](https://github.com/RangaAmirapu/DataEngineeringProjects/tree/master/DataModelingWithPostgres)

## Data Modelling with Apache Cassandra

 **Problem Statement:**
 
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their analytics team is particularly interested in understanding what songs users are listening to. 

Currently, for Sparkify there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

The goal of this project is to create an Apache Cassandra database which can create queries on song play data analysis.

Data modelling is to be done based on the raw data available in CSV format.

ETL pipelines that transfers data from CSV files to Cassandra database are to be developed using python.

[**Go to project**](https://github.com/RangaAmirapu/DataEngineeringProjects/tree/master/DataModellingWithCassandra)
