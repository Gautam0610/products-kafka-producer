import os
import json
import time
import random
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_SASL_MECHANISM = os.getenv('KAFKA_SASL_MECHANISM')
KAFKA_SECURITY_PROTOCOL = os.getenv('KAFKA_SECURITY_PROTOCOL')
KAFKA_SASL_PLAIN_USERNAME = os.getenv('KAFKA_SASL_PLAIN_USERNAME')
KAFKA_SASL_PLAIN_PASSWORD = os.getenv('KAFKA_SASL_PLAIN_PASSWORD')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    sasl_mechanism=KAFKA_SASL_MECHANISM,
    security_protocol=KAFKA_SECURITY_PROTOCOL,
    sasl_plain_username=KAFKA_SASL_PLAIN_USERNAME,
    sasl_plain_password=KAFKA_SASL_PLAIN_PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = ['wireless headphones', 'cotton shirt', 'portable blender', 'mystery novel', 'running shoes']

while True:
    product = random.choice(products)
    product_id = random.randint(100, 999)
    product_data = {
        'product_id': product_id,
        'product_name': product
    }
    
    producer.send(KAFKA_TOPIC, product_data)
    print(f'Sent: {product_data}')
    time.sleep(random.randint(1, 3))
