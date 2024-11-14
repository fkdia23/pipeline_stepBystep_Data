from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import NewTopic
import json

# Créer un client administrateur
admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')

# Tenter de créer le topic
topic_name = "test_topic"
num_partitions = 1
replication_factor = 1

try:
    new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    admin_client.create_topics(new_topics=[new_topic], validate_only=False)
    print(f"Topic '{topic_name}' créé avec succès.")
except TopicAlreadyExistsError:
    print(f"Le topic '{topic_name}' existe déjà.")
except Exception as e:
    print(f"Erreur lors de la création du topic : {e}")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Envoyer des données
for i in range(10):
    data = {'key': f'value-{i}'}
    producer.send(topic_name, key=str(i).encode(), value=data)
    print(f"Envoyé: {data}")

producer.flush()