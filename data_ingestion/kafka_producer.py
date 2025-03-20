import sys
import os
import json
import time
import random
import logging
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Ajouter le répertoire parent au path pour pouvoir importer le générateur
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_generation.web_events_generator import WebEventGenerator

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('web_events_producer')

class KafkaWebEventsProducer:
    def __init__(self, bootstrap_servers, topic_name, use_schema_registry=False, schema_registry_url=None):
        """
        Initialise le producteur Kafka pour les événements web.
        
        Args:
            bootstrap_servers: Liste des serveurs Kafka
            topic_name: Nom du topic Kafka
            use_schema_registry: Utilisation ou non du Schema Registry
            schema_registry_url: URL du Schema Registry si utilisé
        """
        self.topic_name = topic_name
        self.event_generator = WebEventGenerator()
        self.use_schema_registry = use_schema_registry
        
        # Configuration de base du producteur Kafka
        producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'web-events-producer-1'
        }
        
        # Configuration du serializer selon l'utilisation du Schema Registry
        if use_schema_registry and schema_registry_url:
            schema_registry_conf = {'url': schema_registry_url}
            schema_registry_client = SchemaRegistryClient(schema_registry_conf)
            
            # Charger le schéma Avro
            with open('schemas/web_event_schema.avsc', 'r') as f:
                schema_str = f.read()
            
            self.value_serializer = AvroSerializer(schema_registry_client, schema_str)
        else:
            # Utiliser une simple sérialisation JSON si pas de Schema Registry
            self.value_serializer = StringSerializer('utf_8')
            
        self.producer = Producer(producer_config)
        logger.info(f"Producteur Kafka initialisé pour le topic: {topic_name}")
        
    def delivery_callback(self, err, msg):
        """Callback appelé après chaque envoi de message"""
        if err:
            logger.error(f'Échec de livraison du message: {err}')
        else:
            logger.debug(f'Message livré à {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')
            
    def send_event(self, event):
        """Envoie un événement à Kafka"""
        try:
            # Sérialiser l'événement selon la configuration
            if self.use_schema_registry:
                value = self.value_serializer(event)
            else:
                value = json.dumps(event).encode('utf-8')
                
            # Utiliser la catégorie d'événement comme clé pour le partitionnement
            key = event.get('event_type', 'default').encode('utf-8')
            
            # Envoyer le message
            self.producer.produce(
                topic=self.topic_name,
                key=key,
                value=value,
                on_delivery=self.delivery_callback
            )
            self.producer.poll(0)  # Non-bloquant
            
        except Exception as e:
            logger.error(f"Erreur lors de l'envoi de l'événement: {str(e)}")
            
    def simulate_traffic(self, events_per_second=5, duration_seconds=None):
        """
        Simule un trafic web en générant et envoyant des événements à Kafka
        
        Args:
            events_per_second: Nombre moyen d'événements à générer par seconde
            duration_seconds: Durée de la simulation en secondes (None = infini)
        """
        start_time = time.time()
        events_sent = 0
        
        try:
            logger.info(f"Démarrage de la simulation de trafic: {events_per_second} événements/seconde")
            
            while duration_seconds is None or (time.time() - start_time < duration_seconds):
                # Génération d'un nombre variable d'événements pour simuler des pics
                batch_size = max(1, int(random.normalvariate(events_per_second, events_per_second/5)))
                batch_start = time.time()
                
                # Décider si on génère des parcours utilisateur ou des événements individuels
                if random.random() < 0.7:  # 70% des cas: parcours utilisateur
                    journey_length = random.randint(3, 8)
                    events = self.event_generator.generate_user_journey(journey_length)
                    for event in events:
                        self.send_event(event)
                        events_sent += 1
                else:  # 30% des cas: événements individuels
                    for _ in range(batch_size):
                        event = self.event_generator.generate_event()
                        self.send_event(event)
                        events_sent += 1
                
                # Calculer le temps nécessaire pour envoyer ce batch
                elapsed = time.time() - batch_start
                
                # Attendre pour maintenir le débit cible
                sleep_time = max(0, 1 - elapsed)  # 1 seconde moins le temps d'exécution
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
                # Log périodique du statut
                if events_sent % 100 == 0:
                    elapsed_total = time.time() - start_time
                    logger.info(f"Événements envoyés: {events_sent} | "
                                f"Débit: {events_sent/elapsed_total:.2f}/sec")
                    
                # Flush périodique pour garantir la livraison
                if events_sent % 1000 == 0:
                    self.producer.flush()
                    
        except KeyboardInterrupt:
            logger.info("Interruption utilisateur reçue, arrêt de la simulation")
        finally:
            # Flush final pour s'assurer que tous les messages sont envoyés
            remaining = self.producer.flush(10)
            logger.info(f"Simulation terminée - {events_sent} événements envoyés au total")
            if remaining > 0:
                logger.warning(f"{remaining} messages n'ont pas pu être envoyés")

if __name__ == "__main__":
    # Configuration par défaut pour test local
    bootstrap_servers = "localhost:9092"
    topic_name = "web-events"
    
    # Possibilité de surcharger avec des variables d'environnement
    if 'KAFKA_BOOTSTRAP_SERVERS' in os.environ:
        bootstrap_servers = os.environ['KAFKA_BOOTSTRAP_SERVERS']
    if 'KAFKA_TOPIC' in os.environ:
        topic_name = os.environ['KAFKA_TOPIC']
    
    producer = KafkaWebEventsProducer(bootstrap_servers, topic_name)
    producer.simulate_traffic(events_per_second=10)
