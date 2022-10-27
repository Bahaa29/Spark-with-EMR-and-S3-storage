import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as s ,StructField as f , DoubleType as d , IntegerType as i , StringType as str , DateType as d , TimestampType as t

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data =input_data +'song_data/*/*/*/*.json'
    sechema=s([
        f("num_song",i()),
        f("artist_id",str()),
        f("artist_latitude",d()),
        f("artist_longitude",d()),
        f("artist_location",str()),
        f("artist_name",str()),
        f("song_id",str()),
        f("title",str()),
        f("duration",d()),
        f("year",i())
    ])
    # read song data file
    df = spark.read.json(song_data,schema=sechema)
    df.createOrReplaceTempView("song_data_view")
    
    # extract columns to create songs table
    songs_col = ["song_id","title", "artist_id","year", "duration"]
    songs_table=df.select(songs_col).dropDuplicates()
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(output_data+'songs/')

    # extract columns to create artists table
    artists_col =["artist_id", "artist_name", "artist_location ", "artist_latitude", "artist_longitude"] 
    artists_table=df.select(artists_col).dropDuplicates()
    # write artists table to parquet files
    artists_table.write.partitionBy("year","artist_id").parquet(output_data+'artists/')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =input_data +'log_data/*/*/*.json'

    # read log data file
    df =spark.read.json(log_data) 
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    artists_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()
    
    # write users table to parquet files
    artists_table.write.partitionBy("year","artist_id").parquet(output_data+'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, t())
    df = df.withColumn("timestamp",get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x:detatime.fromtimestamp(x),t())
    df = df.withColumn("start_time",get_datetime(df.timestamp))
    
    # extract columns to create time table
    time_table = df.select("start_time").dropDuplicates\
    .withColumn("hour",hour('start_time')).withColumn("day",day("start_time")).withColumn("week",week("start_time"))\
    .withColumn("month", month("start_time")).withColumn("year",year("start_time").withColumn("weekday", date_format("start_time", 'E'))

    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(path=output_data + 'time')
    
    # read in song data to use for songplays table
    song_df = spark.sql("select * from song_data_view")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,(df.song==song_df.title)\
                              &(df.artist==song_df.artist_name)."inner").distinct().select('start_time', 'userId', 'level', 'song_id',\
                                'artist_id', 'sessionId','location','userAgent',\
                                df['year'].alias('year'), df['month'].alias('month')).withColumn("songplay_id", monotonically_increasing_id())
                               
                                                         
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-data-udend/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
