from flask import Flask
from flask import Response
from flask import request
from flask_cors import CORS
from flask import jsonify
from flask_restful import Api, Resource, reqparse
from kafka import KafkaConsumer
from kafka import KafkaProducer
from json import dumps
from json import loads
import os
import re
import random
import logging as log
import configparser
import tweepy
import time

# log.basicConfig(level=log.DEBUG)

app = Flask(__name__)
CORS(app)
api = Api(app)

parser = reqparse.RequestParser()

# configure to be environment variable later
dummy_topic = 'tweets'
bootstrap_servers = 'my-cluster-kafka-bootstrap.amq-streams.svc:9092'

kafka_topic = os.environ['KAFKA_TOPIC']
consumer_key = os.environ['TWTR_CONSUMER_KEY']
consumer_secret = os.environ['TWTR_CONSUMER_SECRET']
access_token = os.environ['TWTR_ACCESS_TOKEN']
access_token_secret = os.environ['TWTR_ACCESS_TOKEN_SECRET']

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
    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:
            print(data)
            #send_data = producer.send(kafka_topic, data)
            #print(send_data)
            return True
        else:
            print('Max seconds reached = ' + str(self.limit))
            print('why still get data', data)
            return False        


    # def on_status(self, status):
    #     data = {
    #         'id': status.id_str,
    #         'tweet': status.text,
    #         'source': status.source,
    #         'retweeted': status.retweeted,
    #         'retweet_count': status.retweet_count,
    #         'created_at': str(status.created_at),
    #         'username': status.user.screen_name,
    #         'user_id': status.user.id_str,
    #         'profile_image_url': status.user.profile_image_url_https,
    #         'followers': status.user.followers_count
    #     }

    #     if (time.time() - self.start_time) < self.limit:
    #         print(data)
    #         #send_data = producer.send(kafka_topic, data)
    #         #print(send_data)
    #         return True
    #     else:
    #         print('Max seconds reached = ' + str(self.limit))
    #         print('why still get data', data)
    #         return False


    # def on_error(self, status_code):
    #     if status_code == 420:
    #         #returning False in on_data disconnects the stream
    #         return False

class Health(Resource):
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

api.add_resource(Health, '/health')
api.add_resource(send_data_to_kafka, '/tweets')
api.add_resource(get_data_from_kafka, '/show')
api.add_resource(twitter_to_kafka, '/twitter_to_kafka')


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
    value_deserializer=lambda x: loads(x.decode('utf-8')))


if __name__ == "__main__":
    app.run(host='0.0.0.0', port='8080')
