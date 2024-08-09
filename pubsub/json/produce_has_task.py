from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from datetime import datetime
import random
import uuid

TOPIC = "stock_avro_topic_has"

def load_avro_schema_from_file():
    key_schema = avro.loads('{"type": "string"}')
    value_schema = avro.load('./schemas/stock_event.avsc')
    return key_schema, value_schema

def produce():
    key_schema, value_schema = load_avro_schema_from_file()

    # Configure the AvroProducer
    producer_config = {
        'bootstrap.servers': '34.101.224.54:19092',
        'schema.registry.url': 'http://34.101.224.54:18081'  # Adjust the URL as needed
    }

    p = AvroProducer(
        producer_config,
        default_key_schema=key_schema,
        default_value_schema=value_schema
    )

    # Produce messages to the topic
    try:
        while True:
            stock = {
                'event_time': datetime.now().isoformat(),
                'ticker': random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV']),
                'price': round(random.random() * 100, 2)
            }
            p.produce(topic=TOPIC, key=str(uuid.uuid4()), value=stock, callback=delivery_report)
            p.poll(0)
    except Exception as e:
        print(str(e))

    # Wait for any outstanding messages to be delivered
    p.flush()

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def main():
    produce()

if __name__ == "__main__":
    main()
