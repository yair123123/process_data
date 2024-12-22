import json
import os
import toolz as t
from kafka import KafkaProducer



@t.curry
def publisher(topic,key,value):
    producer = KafkaProducer(
        bootstrap_servers=os.environ['BOOTSTRAP_SERVER'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    producer.send(
        os.environ[topic],
        value=value,
        key=key.encode('utf-8')
    )