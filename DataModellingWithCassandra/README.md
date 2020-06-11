# Data Modelling with Cassandra

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their analytics team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

## Project Description

The goal of this project is to create an Apache Cassandra database which can create queries on song play data analysis.

Data modelling is to be done based on the raw data available in CSV format.  

ETL pipelines that transfers data from CSV files to Cassandra database are to be developed using python.

## Dataset

Data is available in separate files under event_data directory. 

The event_data folder consists of activity logs in json format. The directory of CSV files are  partitioned by date.

 - event_data/2018-11-08-events.csv
 - event_data/2018-11-09-events.csv
 
Sample data:

    artist,auth,firstName,gender,itemInSession,lastName,length,level,location,method,page,registration,sessionId,song,status,ts,userId
    Survivor,Logged In,Jayden,M,0,Fox,245.36771,free,"New Orleans-Metairie, LA",PUT,NextSong,1.54103E+12,100,Eye Of The Tiger,200,1.54111E+12,101
 
Each file has a header and associated data below it.
 

## Database Modelling

 As we are using Apache Cassandra as our database, we need to think about our queries well in advance as joins are not allowed. One table per query is a great strategy. 

In this project we have three queries to answer. So, we will create three different tables with appropriate primary key, partition keys and clustering columns.

## Project Structure Explanation

 - **event_data** directory contains event day partitioned by date in csv format.
 - **etl.ipynb** reads and processes a single file created by combining all the partitioned event files and loads data into tables for analysis.

## ETL Pipeline Explanation

**Pre processing the data** 

The first step is to create a single file using the partitioned files that will be used to load data into different tables designed as per the query requirements.

This new file is called **event_datafile_new.csv**

**Steps to process the combined data**
*Pre Requisites: Apache Cassandra cluster, Python for ETL*
 - Connect to an Apache Cassandra cluster and create a Keyspace. A keyspace is like an RDBMS database
 - Create tables to each query you want to answer, Queries and explanation are available in etl.ipynb file
 - Insert data into tables using the combined data file.
 - Select data to verify the data

## Project Execution

*Pre Requisites: Apache Cassandra cluster, Python for ETL*

 1.  Run **etl.ipynb** to create the data file and load into Cassandra database. 


