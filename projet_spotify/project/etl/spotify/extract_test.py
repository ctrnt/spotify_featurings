from confluent_kafka import Consumer, KafkaException, KafkaError
import sys

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'spotify-consumer-group',  # Nom du consumer group
    'auto.offset.reset': 'earliest'  # Permet de commencer à lire les messages depuis le début
}

consumer = Consumer(conf)
topic = 'spotify-topic'

# Souscrire au topic
consumer.subscribe([topic])

try:
    while True:
        # Lire un message depuis Kafka
        msg = consumer.poll(1.0)  # Attendre un message pendant 1 seconde
        if msg is None:  # Si aucun message n'est arrivé dans ce délai
            print("Aucun message reçu dans ce délai")
            continue
        if msg.error():  # Si un message a une erreur
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Fin de partition atteinte {msg.partition} à offset {msg.offset}")
            else:
                raise KafkaException(msg.error())
        else:
            print(f"Message reçu : {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    print("Arrêt du consommateur")
finally:
    consumer.close()
