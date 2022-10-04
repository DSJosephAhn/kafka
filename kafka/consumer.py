import json
from kafka import KafkaConsumer

try:
        bootstrap_server= ['localhost:9092']
        topicName= 'test-topic'
        consumer = KafkaConsumer(topicName,
                                bootstrap_servers=bootstrap_server,
                                api_version=(0,11,5),
                                auto_offset_reset='earliest',
                                enable_auto_commit=True,
                                group_id='test_id',
                                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                                consumer_timeout_ms=2000)

        while True:
            for message in consumer:
                print(message.value)
except Exception as e:
        print(e)