import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """Create or retrieve a Spark session
    """
    return SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
                        .getOrCreate()

def process_song_data(spark, input_data, output_data):
    """[summary]

    Args:
        spark ([type]): [description]
        input_data ([type]): [description]
        output_data ([type]): [description]
    """

    song_data = input_data + 'song_data/*/*/*/*.json'
    
    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("title", StringType()),
        StructField("year", IntegerType())
    ])

    df = spark.read.json(song_data, schema=song_schema)

    # song table
    song_table = df.select('title', 'artist_id', 'year', 'duration').dropDuplicates()\
                .withColumn('song_id', monotonically_increasing_id())

    song_table.write.parquet(output_data + 'songs/', mode='overwrite', partitionBy=['year', 'artist_id'])

    # artist table 
    artist_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").dropDuplicates()

    artist_table.write.parquet(output_data + 'artists/', mode='overwrite')

def process_log_data(spark, input_data, output_data):
    """[summary]

    Args:
        spark ([type]): [description]
        input_data ([type]): [description]
        output_data ([type]): [description]
    """

    log_data = input_data + 'log-data/'

    df = spark.read.json(log_data).drop_duplicates()

    df = df.filter(df.page == 'NextSong')

    # user table
    users_fields = ["userId", "firstName", "lastName", "gender", "level"]
    users_table = df.selectExpr(users_fields).drop_duplicates()
    
    users_table.write.parquet(output_data + 'users/', mode='overwrite')

    # time table
    get_timestamp = udf(lambda x: datetime.utcfromtimestamp(int(x) / 1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))

    time_table = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()

    time_table.write.parquet(output_data + 'time_table/', mode='overwrite', partitionBy=['year', 'month'])

    # songplays table
    # read in song data to use for songplays table
    song_df = spark.read\
                .format("parquet")\
                .option("basePath", os.path.join(output_data, "songs/"))\
                .load(os.path.join(output_data, "songs/*/*/"))

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, df.song == song_df.title, how='inner')\
                        .select(monotonically_increasing_id().alias("songplay_id"), col("start_time"),
                                col("userId").alias("user_id"), "level", "song_id", "artist_id", 
                                col("sessionId").alias("session_id"), "location", col("userAgent").alias("user_agent"))

    songplays_table = songplays_table.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
                        .select("songplay_id", songplays_table.start_time, "user_id", "level", "song_id", "artist_id", 
                                "session_id", "location", "user_agent", "year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.drop_duplicates().write.parquet(os.path.join(output_data, "songplays/"), mode="overwrite", partitionBy=["year","month"])

def main():
    spark = create_spark_session()
    input_data = "s3://udacity-spark-project/"
    output_data = "s3://udacity-spark-project/output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
