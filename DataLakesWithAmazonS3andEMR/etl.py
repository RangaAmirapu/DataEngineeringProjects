import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import * 

# read config file
config = configparser.ConfigParser()
config.read('dl.cfg')

# set access keyid and secret as environment variables
os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
        Description: Create and return a spark session
    '''
    # create spark session
    spark = SparkSession \
        .builder \
        .appName("SparkifyDataLake") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
         Description:Process the songs data files, transforms them to create songs, artists table and writes them to partioned parquet files in table directories on S3
         
        :param spark: spark session
        :param input_data: input file path
        :param output_data: output file path
    '''
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates() 

    # extract columns to create songs table 
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').where(col("song_id").isNotNull()).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + 'songs/', mode='overwrite', partitionBy=['year', 'artist_id'])

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').where(col("artist_id").isNotNull()).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    '''
         Description:Process the log data files, transforms them to create users, time, song_plays tables and writes them to partioned parquet files in table directories on S3
         
        :param spark: spark session
        :param input_data: input file path
        :param output_data: output file path
    '''
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data).dropDuplicates() 
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').where(col("userId").isNotNull()).dropDuplicates()
        
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/', mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
       
    # extract columns to create time table
    time_table = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time/', mode='overwrite', partitionBy=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.read\
                .format("parquet")\
                .option("basePath", output_data + "songs/")\
                .load(output_data + "/songs/*/*/")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, df.song == song_df.title, how='inner')\
                        .select(monotonically_increasing_id().alias("songplay_id"),\
                                col("start_time"),\
                                col("userId").alias("user_id"),\
                                "level",\
                                "song_id",\
                                "artist_id",\
                                col("sessionId").alias("session_id"),\
                                "location",\
                                col("userAgent").alias("user_agent"))
    
    # add year and month columns from df for partitioning
    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
                        .select("songplay_id",\
                                songplays_table.start_time,\
                                "user_id",\
                                "level",\
                                "song_id",\
                                "artist_id",\
                                "session_id",\
                                "location",\
                                "user_agent",\
                                "year",\
                                "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays/', mode='overwrite', partitionBy=['year', 'month'])

def main():
    '''
         Description: Call the create_spark_session function, process_song_data and process_log_data functions to create sparkify data lake
    '''
    
    spark = create_spark_session()
    
    # setting fileoutuput committer algorithm versio to 2 to enable fast writes to S3
    # more Info : https://kb.databricks.com/data/append-slow-with-spark-2.0.0.html
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    
    # specify input data folder
    input_data = config['SPARKIFY_DATALAKE_INPUT_DATA_PATH']
    
    # specify output data folder
    # Make sure the user has write access to S3 bucket
    output_data = config['SPARKIFY_DATALAKE_OUTPUT_DATA_PATH']
    
    # process song data
    process_song_data(spark, input_data, output_data)
    
    # process log data
    process_log_data(spark, input_data, output_data)
    
    # close spark session
    spark.close()


if __name__ == "__main__":
    main()
