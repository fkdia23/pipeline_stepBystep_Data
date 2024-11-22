from confluent_kafka import Producer
import pandas as pd
from json import dumps
import time

# Configuration de Kafka
KAFKA_TOPIC_NAME = "test_topic"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Fonction de callback pour gérer les erreurs ou succès d'envoi
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

if __name__ == "__main__":
    print("Kafka Producer Application Started ...")

    # Configuration du producteur Kafka
    kafka_producer = Producer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
    })

    # Chemin du fichier CSV
    file_path = "weather-consumer/datasets/orders.csv"
    
    # Lecture du fichier CSV avec pandas
    orders_pd_df = pd.read_csv(file_path)
    print(orders_pd_df.head(1))  # Afficher les premières lignes pour vérifier

    # Conversion des données en une liste de dictionnaires
    orders_list = orders_pd_df.to_dict(orient="records")
    print(orders_list[0])  # Afficher le premier élément pour vérification

    # Envoi des messages à Kafka
    for order in orders_list:
        message = dumps(order)  # Sérialisation du dictionnaire en JSON
        print("Message to be sent: ", message)
        kafka_producer.produce(
            topic=KAFKA_TOPIC_NAME,
            value=message,
            callback=delivery_report
        )
        kafka_producer.flush()  # Forcer l'envoi immédiat
        time.sleep(1)

    print("Kafka Producer Application Finished.")
