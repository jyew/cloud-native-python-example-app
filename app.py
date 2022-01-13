from flask import Flask
from flask import Response
from flask import request
from flask_cors import CORS
from flask import jsonify
from flask_restful import Api, Resource, reqparse
from kafka import KafkaConsumer
from kafka import KafkaProducer
from kafka import TopicPartition
from json import dumps
from json import loads
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson import json_util
import os
import re
import random
import logging as log
import configparser
import tweepy
import time
import datetime
import pprint

# log.basicConfig(level=log.DEBUG)

app = Flask(__name__)
CORS(app)
api = Api(app)

parser = reqparse.RequestParser()

# configure to be environment variable later
dummy_topic = 'tweets'
bootstrap_servers = 'my-cluster-kafka-bootstrap.amq-streams.svc:9092'
mongodb_host = 'mongodb:27017'

kafka_topic = os.environ['KAFKA_TOPIC']
consumer_key = os.environ['TWTR_CONSUMER_KEY']
consumer_secret = os.environ['TWTR_CONSUMER_SECRET']
access_token = os.environ['TWTR_ACCESS_TOKEN']
access_token_secret = os.environ['TWTR_ACCESS_TOKEN_SECRET']

# mongo credentials
mongodb_user = os.environ['MONGODB_USER']
mongodb_password = os.environ['MONGODB_PASSWORD']
mongodb_db_name = os.environ['MONGODB_DATABASE']
mongodb_collection_name = 'twitter_collection'
mongoclient = MongoClient(host='mongodb', port=27017, 
                        username=mongodb_user,
                        password=mongodb_password, 
                        authSource=mongodb_db_name)
db = mongoclient[mongodb_db_name]
collection = mongoclient[mongodb_db_name][mongodb_collection_name]

# twitter authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api_twitter = tweepy.API(auth)

# If the authentication was successful, this should print the
# screen name / username of the account
print(api_twitter.verify_credentials().screen_name)


@app.route('/')
def index():
    # return render_template("index.html")
    return "This is the most amazing app EVER."
 
class MyStreamListener(tweepy.Stream):
    """ make default streaming from Twitter for 10s """

    def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret, time_limit=10):
        # consumer_key, consumer_secret, access_token, access_token_secret, 
        self.start_time = time.time()
        self.limit = time_limit        
        super(MyStreamListener, self).__init__(consumer_key, consumer_secret,
                                                access_token, access_token_secret,) 

    def on_status(self, status):
        data = {
            'id': status.id_str,
            'tweet': status.text,
            'source': status.source,
            'retweeted': status.retweeted,
            'retweet_count': status.retweet_count,
            'created_at': str(status.created_at),
            'username': status.user.screen_name,
            'user_id': status.user.id_str,
            'profile_image_url': status.user.profile_image_url_https,
            'followers': status.user.followers_count
        }

        if (time.time() - self.start_time) < self.limit:
            print(data)
            send_data = producer.send(kafka_topic, data)
            print(send_data)
            return True
        else:
            print('Max seconds reached = ' + str(self.limit))
            self.running = False
            return False

    def on_error(self, status_code):
        if status_code == 420:
            self.running = False
            return False

class health(Resource):
    def get(self):
        return "Health_OK"

class send_data_to_kafka(Resource):
    def get(self):
        data = {
            'id': 'jyew',
            'value': 'test',
        }     
        producer.send(dummy_topic, data)
        producer.flush()
        print('posted to kafka:', data)
        return 200


class get_data_from_kafka(Resource):
    def get(self):
        for message in consumer:
            print(message)
        return message.value

class twitter_to_kafka(Resource):
    def get(self):
        parser.add_argument('keyword', action='append', type=str)
        parser.add_argument('seconds', type=int)
        args = parser.parse_args()
        print('args', args['keyword'])
        global track_keywords

        if args['keyword'] is not None:
            track_keywords = args['keyword']
        if args['seconds'] is not None:
            time_limit = args['seconds']

        myStream = MyStreamListener(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token=access_token,
            access_token_secret=access_token_secret,
            time_limit=time_limit
        )
        myStream.filter(track=track_keywords, languages=["en"])
        return 200 

class test_mongodb(Resource):
    def get(self):
        collection = mongoclient[mongodb_db_name][mongodb_collection_name]
        message = {"author": "Jordan",
                    "text": "My first blog post!",
                    "tags": ["mongodb", "python", "pymongo"],
                    "date": datetime.datetime.utcnow()}
        # write data
        message_id = collection.insert_one(message).inserted_id
        print(message_id)
        print(mongoclient[mongodb_db_name].list_collection_names())
        
        # read data
        for post in collection.find():
            pprint.pprint(post)

        # erase data
        collection.delete_many({"author": "Jordan"})
        return 200

class test_mongodb2(Resource):
    def get(self):
        collection = mongoclient[mongodb_db_name][mongodb_collection_name]
        # message = {"author": "Jordan",
        #             "text": "My first blog post!",
        #             "tags": ["mongodb", "python", "pymongo"],
        #             "date": datetime.datetime.utcnow()}
        # # write data
        # message_id = collection.insert_one(message).inserted_id
        # print(message_id)
        # print(mongoclient[mongodb_db_name].list_collection_names())
        
        # # read data
        # for post in collection.find():
        #     pprint.pprint(post)

        # # erase data
        # collection.delete_many({"author": "Jordan"})

        parser.add_argument('keyword', action='append', type=str)
        parser.add_argument('action', type=str)
        args = parser.parse_args()
        if args['keyword'] is not None:
            track_keywords = args['keyword']
        for keyword in track_keywords:
            query = {"tweet": {"$regex": keyword, "$options": "gim"}}
            for doc in collection.find(query):
                print(doc)
        return 200


class test_mongodb3(Resource):
    def get(self):
        collection = mongoclient[mongodb_db_name][mongodb_collection_name]

        # erase data
        collection.delete_many()
        return 200

# continue dev here
class kafka_to_mongodb(Resource):
    def get(self):
        parser.add_argument('keyword', action='append')
        args = parser.parse_args()
        global track_keywords
        db_item = {}
        if args['keyword'] is not None:
            track_keywords = args['keyword']
        countDocsWritten = 0
        collection = mongoclient[mongodb_db_name][mongodb_collection_name]

        tp = TopicPartition(kafka_topic,0)
        # obtain the last offset value
        lastOffset = consumer.end_offsets([tp])[tp]
        print("lastoffset", lastOffset)

        for message in consumer:
            msg = message.value
            tidy_tweet = msg['tweet'].strip().encode('ascii', 'ignore').decode('utf-8')
            print(tidy_tweet)
            countDocsWritten = countDocsWritten + 1
            if len(tidy_tweet) <= 5:
                break
            for keyword in track_keywords:
                if len(re.findall(keyword,tidy_tweet)) > 0:
                    db_item['_id'] = ObjectId()
                    db_item['keyword'] = keyword
                    db_item['tweet'] = tidy_tweet
                    db_item['created_at'] = msg['created_at']
                    collection.insert_one(db_item)
                    countDocsWritten = countDocsWritten + 1
                    print('\nWritten %d documents to MongoDb' % (countDocsWritten))
                    print(db_item)            
            # if message.offset == lastOffset - 1:
            #     break
        data = {'message': 'saved {} messages'.format(countDocsWritten), 'code': 'SUCCESS'}
        return jsonify(data)

class get_db_data1(Resource):
    def get(self):
        collection = mongoclient[mongodb_db_name][mongodb_collection_name]
        data = {}
        parser.add_argument('keyword', action='append')
        args = parser.parse_args()
        global track_keywords
        if args['keyword'] is not None:
            track_keywords = args['keyword']
        data["labels"] = track_keywords
        data["values"] = []
        data["messages"] = []
        for keyword in track_keywords:
            query = {"tweet": {"$regex": keyword, "$options": "gim"}}
            count = collection.count_documents(query)
            data["values"].append(count)

            # db.products.find().sort({"created_at": 1}) 

            # json serializing mongo documents
            for doc in collection.find(query):
                data["messages"].append(dumps(doc, default=json_util.default))
            # data["messages"] = [dumps(doc, default=json_util.default) for doc in collection.find(query)]
        return data 

class get_db_data2(Resource):
    # generate random results for rendering first
    def get(self):
        collection = mongoclient[mongodb_db_name][mongodb_collection_name]
        data = {}
        parser.add_argument('keyword', action='append')
        args = parser.parse_args()
        global track_keywords
        if args['keyword'] is not None:
            track_keywords = args['keyword']
        sentiments = ['positive', 'negative', 'neutral']
        data["labels"] = track_keywords
        data["datasets"] = [
            {
                'label': sentiments[0],
                'data': [],
                'backgroundColor': '#D6E9C6',
            },
            {
                'label': sentiments[1],
                'data': [],
                'backgroundColor': '#FAEBCC',
            },
            {
                'label': sentiments[2],
                'data': [],
                'backgroundColor': '#EBCCD1',
            }
        ]

        
        # for keyCount, keyword in enumerate(track_keywords):
        #     for sentiCount, sentiment in enumerate(sentiments):
        #         count = collection.count_documents({"polarity": sentiment, "keyword": keyword})
        #         data["datasets"][sentiCount]["data"].append(count)

        # randomly generate fake results when we don't have sentiment model yet
        for keyword in track_keywords:
            query = {"tweet": {"$regex": keyword, "$options": "gim"}}
            count = collection.count_documents(query)

            count_positive = random.randint(0, count)
            count_negative = random.randint(0, count - count_positive)
            count_neutral = count - count_positive - count_negative

            data["datasets"][0]["data"].append(count_positive)
            data["datasets"][1]["data"].append(count_negative)
            data["datasets"][2]["data"].append(count_neutral)
        return data

class apply_sentiment(Resource):
    def get(self):
        pass
        # collection = mongoclient[mongodb_db_name][mongodb_collection_name]
        # for count in range(1,collection.count()):
        #     record = collection.find().limit(-1).skip(random.randint(1,collection.count())).next()   
        #     sentiment_analysis_result = client.Sentiment({'text': record['tweet']})
        #     collection.update_one({'_id':record['_id']},
        #                             { "$set" : {'polarity': sentiment_analysis_result['polarity'] } })
        #     print(record)


api.add_resource(health, '/health')
api.add_resource(send_data_to_kafka, '/tweets')
api.add_resource(get_data_from_kafka, '/show')
api.add_resource(twitter_to_kafka, '/twitter_to_kafka')
api.add_resource(test_mongodb, '/test_mongo')
api.add_resource(test_mongodb2, '/test_mongo2')
api.add_resource(test_mongodb3, '/test_mongo3')
api.add_resource(kafka_to_mongodb, '/kafka_to_mongodb')
api.add_resource(get_db_data1, '/get_db_data1')
api.add_resource(get_db_data2, '/get_db_data2')
api.add_resource(apply_sentiment, '/apply_sentiment')

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: dumps(x).encode('utf-8'),
)

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    # group_id='consumer_group_1',
    enable_auto_commit=True,
    value_deserializer=lambda m: loads(m.decode('utf-8')))


if __name__ == "__main__":
    app.run(host='0.0.0.0', port='8080')
