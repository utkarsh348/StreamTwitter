import os
import json
import pymongo
from json import loads
from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient

load_dotenv()


def get_database():
    CONNECTION_STRING = f"mongodb+srv://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASS')}@cluster0.ehfie.mongodb.net/DBTProject?retryWrites=true&w=majority"
    client = MongoClient(CONNECTION_STRING)
    return client.DBTProject


HASHTAGS = os.getenv("HASHTAGS").split()
HASHTAGS = [i[1:] for i in HASHTAGS]
consumer = KafkaConsumer(*HASHTAGS)

dbs = get_database()
loc = dict()
for message in consumer:
    data = message.value
    data = json.loads(data.decode('utf-8'))
    if data["hashtag"] not in loc:
        loc[data["hashtag"]] = {
            "start": data["start_time"],
            "end": data["end_time"],
            "count": data["count"]
        }
    else:
        if data["start_time"] == loc[data["hashtag"]]["start"]:
            loc[data["hashtag"]]["count"] += data["count"]
        elif loc[data["hashtag"]]["start"] != '' and data["start_time"] != loc[data["hashtag"]]["start"]:
            ins = {data["hashtag"]: loc[data["hashtag"]]}
            dbs['IPLData'].insert_one(ins)
            print("Inserted")
            print(ins)
            loc[data["hashtag"]]["start"] = data["start_time"]
            loc[data["hashtag"]]["end"] = data["end_time"]
            loc[data["hashtag"]]["count"] = data["count"]
