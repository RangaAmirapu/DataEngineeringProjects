# Data Warehousing using AWS Redshift

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Their analytics team is particularly interested in understanding what songs users are listening to. Their user base and song database has grown and want to move their processes and data onto the cloud. 

## Project Description

The goal of this project is to develop a data model and ETL process for song play analysis.

Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. ETL pipeline has to be built that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to finding insights.

Staging tables, facts and dimension tables are to be defined in star schema using Amazon Redshift.

ETL pipelines that transfers data from files in json format to Amazon Redshift database are to be developed using python.

## Datasets

Data is available in two separate folders in s3 under log_data and song_data folders.

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

![Sparkify star schema](https://raw.githubusercontent.com/RangaAmirapu/DataEngineeringProjects/master/DataWarehousingWithAmazonRedshift/Images/sparkify_erd.png)

### Staging Tables
**staging_events** - Used as interim table for log data before data is logged into analytical tables. All the data from log_data folder is loaded into this table and then moved into facts and dimensions as per the rules.
**staging_songs** - Used as interim table for song data before data is logged into analytical tables. All the data from song_data folder is loaded into this table and then moved into facts and dimensions as per the rules.

### Fact Table
**songplays** -  Records log data associated with song plays (records with page NextSong). songplays table uses distribution style as KEY and the distribution key and sort key is start_time. Using this rows with similar values are placed int the same slice as our time dimension can grow real big.

### Dimension Tables
**users** - users in the app (user_id, first_name, last_name, gender, level). users table uses distribution style as AUTO and sortkey is user_id. Using this Redshift intelligently takes care of distribution depending on the data size, data is sorted by user_id in the table

**songs** - songs in music database (song_id, title, artist_id, year, duration). songs table uses distribution style as AUTO and sortkey is song_id. Using this Redshift intelligently takes care of distribution depending on the data size, data is sorted by song_id in the table

**artists** - artists in music database (artist_id, name, location, latitude, longitude). artists table uses distribution style as AUTO and sortkey is artist_id. Using this Redshift intelligently takes care of distribution depending on the data size, data is sorted by artist_id in the table

**time** - timestamps of records in songplays broken down into specific units (start_time, hour, day, week, month, year, weekday). time table uses distribution style as KEY and the distribution key and sort key is start_time. Using this rows with similar values are placed int the same slice as our time dimension can grow real big.
 
## Project Structure Explanation

 - **sql_queries.py** contains all sql queries
 - **create_tables.py** drops and creates tables. Used to reset the tables each time before running etl scripts
 - **etl.py** reads and processes all files from song_data and log_data and loads them into staging tables and then loads the data from staging tables to analytical tables
 - **dwh.cfg** configuration file for AWS
 - **RedshiftCluster- IaC.ipynb** Iac file to create Redshift cluster using AWS python SDK

## ETL Pipeline Explanation

Data is available in two separate folders under data directory in log_data and song_data in S3

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

 - Using S3 **copy**  command, load all the data from log_data to staging_events table
 - A JSONPaths file that COPY uses to parse the JSON source data is also available by name log_json_path.json
 
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

 - Using S3 **copy**  command, load all the data from song_data to staging_songs table
---
 - **Loading songplays table** :
	 - Extract ***start_time, user_id, level, session_id, location, user_agent*** from staging_events table for records with page **NextSong**
      - Using ***song, artist***  fields from staging_events table find ***song_id, artist_id*** from staging_songs  by joining staging_songs and staging_events tables on **title** and **artist_name** and load into songplays table

 - **Loading users table** :
	 - Extract  and load ***user_id, first_name, last_name, gender, level*** from staging_events table for records with page **NextSong** and userId is not null

 - **Loading songs table** :
	 - Extract  and load ***song_id, title, artist_id, year, duration*** from staging_songs table for records with song_id is not null
	 
 - **Loading artists table** :
	 - Extract  and load ***artist_id, name, location, latitude, longitude*** from staging_songs table for records with artist_id is not null
	 
- **Loading time table** :
  - Extract distinct ***ts*** (timestamp) from staging_events table and calculate the respective ***start_time, hour, day, week, month, year, weekday*** using the ***ts*** value and load into time table

***Data quality checks are enforced to all tables where ever possible***

## Project Execution

 1. Set up required AWS configuration values in dwh.cfg
 2. Run RedshiftCluster- IaC.ipynb to create a redshift cluster if not available
 3. Run **create_tables.py** to connect to the database and create the tables mentioned in sql_queries.py
 4. Run **etl.py** to load data into staging tables and analytical tables

## Example Queries

 - **Find the top 5 songs played**
 
    *select s.title, count(s.title) as songplay_count 
    from public.songplays sp inner join public.songs s 
    on sp.song_id = s.song_id 
    group by s.title 
    order by songplay_count desc
     limit 5*
     
  ![top 5 songs palyed](https://raw.githubusercontent.com/RangaAmirapu/DataEngineeringProjects/master/DataWarehousingWithAmazonRedshift/Images/top5_songs_played.PNG)

 - **Find the top 5 artists**
 
	 *select a.name as artist_name, count(a.name) as artist_play_count 
	 from public.songplays sp inner join public.artists a 
	 on sp.artist_id = a.artist_id 
	 group by a.name 
	 order by artist_play_count desc 
	 limit 5*

![Top 5 artists played](https://raw.githubusercontent.com/RangaAmirapu/DataEngineeringProjects/master/DataWarehousingWithAmazonRedshift/Images/top5_artists_played.PNG)
