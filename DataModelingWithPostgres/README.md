# Data Modelling with Postgres

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their analytics team is particularly interested in understanding what songs users are listening to.

## Project Description

The goal of this project is to develop a data model and ETL process for song play analysis

Data modelling is to be done based on the raw data available in json format. Facts and dimension tables are to be defined in star schema using Postgres database

ETL pipelines that transfers data from files in json format to Postgres database are to be developed using python

## Datasets

Data is available in two separate folders under data directory in log_data and song_data

### Log Data
The log_data folder consists of activity logs in json format. The log files are partioned by year and month.

 - log_data/2018/11/2018-11-12-events.json
  - log_data/2018/11/2018-11-13-events.json

Sample data:

    {"artist":null,"auth":"Logged In","firstName":"Walter","gender":"M","itemInSession":0,"lastName":"Frye","length":null,"level":"free","location":"San Francisco-Oakland-Hayward, CA","method":"GET","page":"Home","registration":1540919166796.0,"sessionId":38,"song":null,"status":200,"ts":1541105830796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"39"}

### Song Data
Each file in song_data folder contains metadata about a song and the artist of the song. The files are partitioned by first three letters of each song's track ID

- song_data/A/B/C/TRABCEI128F424C983.json
- song_data/A/A/B/TRAABJL12903CDCF1A.json

Sample Data 

    {"num_songs": 1, "artist_id": "ARD7TVE1187B99BFB1", "artist_latitude": null, "artist_longitude": null, "artist_location": "California - LA", "artist_name": "Casual", "song_id": "SOMZWCG12A8C13C480", "title": "I Didn't Mean To", "duration": 218.93179, "year": 0}

## Database Schema

 The schema design used for this project is star schema with one fact table and four dimension tables
 
 Star Schema is suitable for this analysis because:
 - The data will de normalized and it helps in faster reads
 - Queries will be simpler and better performing as there are lesser joins
 - We don't have any many to many relationships

![Star Schema for Sparkify](https://github.com/RangaAmirapu/DataEngineeringProjects/blob/master/DataModelingWithPostgres/sparkify_erd.png?raw=true)

### Fact Table
**songplays** -  Records log data associated with song plays (records with page NextSong)

### Dimension Tables

**users** - users in the app (user_id, first_name, last_name, gender, level)

**songs** - songs in music database (song_id, title, artist_id, year, duration)

**artists** - artists in music database (artist_id, name, location, latitude, longitude)

**time** - timestamps of records in songplays broken down into specific units (start_time, hour, day, week, month, year, weekday)

 

## Project Structure Explanation

 - **data** directory contains log_data and song_data datasets.
 - **sql_queries.py** contains all sql queries
 - **create_tables.py** drops and creates tables. Used to rest the tables each time before running etl scripts
 - **test.ipynb** displays first few rows of each table, used to check each table
 - **etl.ipynb** reads and processes a single file from song_data and log_data and loads the data into your tables
 - **etl.py** reads and processes all files from song_data and log_data and loads them into database tables

## ETL Pipeline Explanation

Data is available in two separate folders under data directory in log_data and song_data

**Process Song Data** - Each file in song_data folder contains metadata about a song and the artist of the song. 

Sample Data :

    {
	   "num_songs": 1, 
	   "artist_id": "ARD7TVE1187B99BFB1", 
	   "artist_latitude": null, 
	   "artist_longitude": null, 
	   "artist_location": "California - LA", 
	   "artist_name": "Casual", 
	   "song_id": "SOMZWCG12A8C13C480", 
	   "title": "I Didn't Mean To", 
	   "duration": 218.93179, 
	   "year": 0
    }

 - Extract ***song_id, title, artist_id, year, duration*** from each file and insert into songs table
 - Extract ***artist_id, artist_name, artist_location, artist_latitude, artist_longitude*** from each file and insert into artists table


**Process Log Data** - Each file in song_data folder contains metadata about a song and the artist of the song. 

Sample data:

    {
	   "artist":null,
	   "auth":"Logged In",
	   "firstName":"Walter",
	   "gender":"M",
	   "itemInSession":0,
	   "lastName":"Frye",
	   "length":null,
	   "level":"free",
	   "location":"San Francisco-Oakland-Hayward, CA",
	   "method":"GET",
	   "page":"Home",
	   "registration":1540919166796.0,
	   "sessionId":38,
	   "song":null,
	   "status":200,
	   "ts":1541105830796,
	   "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
	   "userId":"39"
	 }

 - Extract ***ts*** from each entry, extract the ***start_time, hour, day, week, month, year, weekday*** using the ***ts*** value and insert into time table
 -  Extract ***user_id, first_name, last_name, gender, level*** from each file and insert into users table
 - --
 - **Loading songplays table :**   Extract ***start_time, user_id, level, session_id, location, user_agent*** from log data
   - Using ***song, artist, length***  fields find ***song_id, artist_id*** from songs and artists tables and insert the data into songplays table

## Project Execution

*Pre Requisite Softwares: Postgres for database, Python for ETL*

 1. Run **create_tables.py** to create your database and tables
 2. Run **etl.py** to extract the data from data folder and load into tables
 3. Run **test.ipynb** to verify data load
