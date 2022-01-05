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

app = Flask(__name__)
CORS(app)
api = Api(app)

parser = reqparse.RequestParser()

# configure to be environment variable later
kafka_topic = 'tweets'
bootstrap_servers = 'my-route-without-auth-amq-streams.apps.cluster-4987.4987.sandbox1668.opentlc.com:80'


@app.route('/')
def index():
    # return render_template("index.html")
    return "This is the most amazing app EVER."
 

class Health(Resource):
    def get(self):
        return "Health_OK"

class send_data_to_kafka(Resource):
    def get(self):
        data = {'latest_tweet': 'dummy'}
        producer.send(kafka_topic, value=data)
        producer.flush()


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
