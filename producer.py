from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:29092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
producer.send('offering_new', {"dataObjectID": "test3"})
producer.flush()