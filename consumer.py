from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(bootstrap_servers='localhost:29092',auto_offset_reset='earliest', value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.subscribe(['offering_new'])
for message in consumer :
    print(message)
