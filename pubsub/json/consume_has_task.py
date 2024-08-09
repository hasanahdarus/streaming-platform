import json
from confluent_kafka import KafkaException
from confluent_kafka.avro import AvroConsumer
from confluent_kafka import avro 

def consume():
    config = {
        "bootstrap.servers": "34.101.224.54:19092",
        "schema.registry.url": "http://34.101.224.54:18081",
        "group.id": "has_task_avro",
        "auto.offset.reset": "earliest"
    }


    consumer = AvroConsumer(config)
    consumer.subscribe(["clickstream"])

    try:
        while True:
            try:
                msg = consumer.poll(1)

                if msg is None:
                    continue

                print("Key is :" + json.dumps(msg.key()))
                print("Value is :" + json.dumps(msg.value()))
                print("-------------------------")

            except KafkaException as e:
                print('Kafka failure ' + e)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def main():
    consume()

if __name__ == '__main__':
    main()