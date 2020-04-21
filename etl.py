import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import month

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

time_query = """
    SELECT DISTINCT timestamp as start_time, 
            hour(timestamp) as hour, 
            day(timestamp) as day, 
            weekofyear(timestamp) as week, 
            month(timestamp) as month, 
            year(timestamp) as year, 
            weekday(timestamp) as weekday
        FROM STAGING_EVENTS
"""

log_filter_query = """
    SELECT *, CAST(ts/1000 as Timestamp) as timestamp 
    FROM STAGING_EVENTS 
    WHERE page = 'NextSong'
"""

""" Create the spark Session
"""


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


""" Process the data song
"""


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song-data/*/*/*/*.json'

    # local test
    # song_data = input_data + 'song-data/C/A/A/*.json'

    # read song data file
    df = spark.read.json(song_data)

    df.printSchema()
    # df.show(50)

    # extract columns to create songs table
    songs_table = df["song_id", "title", "artist_id", "year", "duration"].dropDuplicates()
    songs_table.show(5)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs.parquet', mode='overwrite')

    # extract columns to create artists table
    artists_table = df["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]. \
        withColumnRenamed("artist_name", "name"). \
        withColumnRenamed("artist_location", "location"). \
        withColumnRenamed("artist_latitude", "latitude"). \
        withColumnRenamed("artist_longitude", "longitude").dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists.parquet', mode='overwrite')


""" Process the log data
"""


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)

    df.createOrReplaceTempView("STAGING_EVENTS")
    df = spark.sql(log_filter_query)
    df.createOrReplaceTempView("STAGING_EVENTS")

    # filter by actions for song plays
    songplays_table = df['ts', 'userId', 'level', 'sessionId', 'location', 'userAgent']

    # extract columns for users table
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level'].dropDuplicates()

    users_table.printSchema()

    # write users table to parquet files
    users_table.write.parquet(output_data + 'users.parquet', mode='overwrite')

    # extract columns to create time table
    time_table = spark.sql(time_query)

    time_table.printSchema()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time.parquet', mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/songs.parquet")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, song_df.title == df.song)

    songplays_table = songplays_table["ts", "userId", "level", "song_id", "artist_id",
                                      "sessionId", "location", "userAgent", "year", month(
        "timestamp").alias('month')]. \
        withColumnRenamed("userId", "user_id"). \
        withColumnRenamed("sessionId", "session_id"). \
        withColumnRenamed("userAgent", "user_agent").dropDuplicates()

    songplays_table.printSchema()
    songplays_table.show(50)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays.parquet'),
                                                               mode='overwrite')


def main():
    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://output-data-lake/"

    # local output test
    # output_data = "output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
