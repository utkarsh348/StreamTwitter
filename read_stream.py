import os
import json
import time
import logging
import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import window
from kafka import KafkaProducer
from dotenv import load_dotenv
from pyspark.streaming import StreamingContext

load_dotenv()

# logging.basicConfig(
#     format='%(asctime)s - %(name)s - %(levelname)s - - %(filename)s - %(funcName)s - %(lineno)d -  %(message)s',
#     level=logging.DEBUG,
#     filename='spark.log',
#     filemode='w'
# )


def create_topics(hashtags):
    for i in hashtags:
        os.system(
            '/home/varun/Downloads/kafka_2.12-2.8.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic {}'.format(i[1:]))
        os.system(
            '/home/varun/Downloads/kafka_2.12-2.8.0/bin/kafka-configs.sh --zookeeper <zkhost>:2181 --entity-type topics --alter --entity-name {} --add-config retention.ms=60000'.format(i[1:]))


def get_hashtag_dataframes(df):
    dfs = {hashtag: df.filter(df.tweet.contains(hashtag))
           for hashtag in HASHTAGS}
    return dfs


def get_tumbling_window_dataframes(hashtag_dfs):
    # obtain tumbling window dataframes
    windows = dict()
    for hashtag, hashtag_df in hashtag_dfs.items():
        w = hashtag_df.groupBy(
            window(timeColumn="time", windowDuration="30 seconds")).count().alias("count")
        windows[hashtag] = w
    return windows


def process_stream(tweets):
    tweets = tweets.collect()
    if not tweets:
        return

    tweets = [json.loads(tweet) for tweet in tweets]

    result = list()
    for tweet in tweets:
        tweet_time = datetime.datetime.strptime(
            tweet["time"], "%a %b %d %H:%M:%S %z %Y")
        result.append((tweet_time, tweet["tweet"]))

    df = spark_sql_context.createDataFrame(
        result, ["time", "tweet"]).toDF("time", "tweet")

    hashtag_dfs = get_hashtag_dataframes(df)

    tumbling_window_dfs = get_tumbling_window_dataframes(hashtag_dfs)
    publish(tumbling_window_dfs)


def publish(latest_count):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    for hashtag, df in latest_count.items():
        num = 0
        start = ""
        end = ""

        if len(df.collect()) == 0:
            num = 0
        else:
            num = df.collect()[0][1]
            start = df.collect()[0][0]["start"].strftime("%Y-%m-%d %H:%M:%S")
            end = df.collect()[0][0]["end"].strftime("%Y-%m-%d %H:%M:%S")
        data = {
            "hashtag": hashtag,
            "count": num,
            "start_time": start,
            "end_time": end
        }
        print(data)
        producer.send(hashtag[1:], data)


if __name__ == "__main__":
    HASHTAGS = os.getenv("HASHTAGS").split()
    STREAM_HOSTNAME = os.getenv("STREAM_HOSTNAME")
    STREAM_PORT = int(os.getenv("STREAM_PORT"))

    spark_context = SparkContext.getOrCreate()
    spark_streaming_context = StreamingContext(spark_context, 5)
    spark_sql_context = SQLContext(spark_context)

    spark_streaming_context\
        .socketTextStream(STREAM_HOSTNAME, STREAM_PORT)\
        .foreachRDD(process_stream)

    spark_streaming_context.start()
    spark_streaming_context.awaitTermination(1000)
    spark_streaming_context.stop()
