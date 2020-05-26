import sys
import time
import os
import json
import random
from dateutil import parser
from confluent_kafka import Producer

import names # https://pypi.org/project/names/
import random


TOPIC = 'users'

cities = ["Mountain View", "San Francisco", "Daly City", "San Mateo", "San Bruno", "Millbrae", "Burlingame", "Foster City", "Belmont", "San Carlos"]
is_customer=["true", "false"]

producer = Producer({
    'bootstrap.servers': '<your-bootstrap-server>',
    'broker.version.fallback': '0.10.0.0',
    'api.version.fallback.ms': 0,
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '<your-kafka-api-key>',
    'sasl.password': '<your-kafka-api-secret>'
})

for i in range (5000):
    msg ='"name":"{}", "address":"{}", "age":{}, "is_customer":{}'.format(names.get_full_name(), cities[i%10], random.randint(20,40), is_customer[i%2])
    # create a json object
    msg = '{' + msg + '}'
    assert json.loads(msg)
    producer.produce(TOPIC, msg)
    producer.flush()