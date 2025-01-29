from kafka import KafkaConsumer
from configs import kafka_config
import json

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda x: json.loads(x.decode("utf-8")) if x else None,
    key_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_consumer_group_3'
)

my_name = "kari"
topic_name = f'{my_name}_athlete_summary'

consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")

try:
    for message in consumer:
        print(f"Received message: {message.value}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()