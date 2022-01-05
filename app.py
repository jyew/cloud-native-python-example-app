from flask import Flask
from flask import Response
from flask import render_template
from flask import request
from flask_cors import CORS
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

log.basicConfig(level=log.DEBUG)

app = Flask(__name__)
CORS(app)
api = Api(app)

parser = reqparse.RequestParser()

# configure to be environment variable later
kafka_topic = 'tweets'
bootstrap_servers = 'my-cluster-kafka-bootstrap.amq-streams.svc:9092'


@app.route('/')
def index():
    # return render_template("index.html")
    return "This is the most amazing app EVER."
 

class Health(Resource):
    def get(self):
        return "Health_OK"

class send_data_to_kafka(Resource):
    def get(self):
        data = {
            'id': 'jyew',
            # 'tweet': status.text,
            # 'source': status.source,
            # 'retweeted': status.retweeted,
            # 'retweet_count': status.retweet_count,
            # 'created_at': str(status.created_at),
            'username': 'jyew',
            # 'user_id': status.user.id_str,
            # 'profile_image_url': status.user.profile_image_url_https,
            # 'followers': status.user.followers_count
        }     

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: dumps(x).encode('utf-8'),
            api_version=(0, 10, 2)
        )


        producer.send(kafka_topic, data)
        producer.flush()
        print('posted to kafka:', data)
        return 200

    # def post(self):
    #     args = parser.parse_args()
    #     todos[todo_id] = request.form['data']
    #     return {todo_id: todos[todo_id]}


api.add_resource(Health, '/health')
api.add_resource(send_data_to_kafka, '/tweets')


producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: dumps(x).encode('utf-8'),
    api_version=(0, 10, 2)
)




if __name__ == "__main__":
    app.run(host='0.0.0.0', port='8080')
