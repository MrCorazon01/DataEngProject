import sys
import os
import json
import time
import logging
from datetime import datetime
from pathlib import Path
from confluent_kafka import Consumer, KafkaError, KafkaException
import threading

# Ajouter le répertoire parent au path pour pouvoir importer les configurations
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.kafka_config import KAFKA_CONFIG, TOPICS, DATA_LAKE_CONFIG

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('web_events_consumer')

class KafkaWebEventsConsumer:
    """
    Consommateur Kafka pour les événements web qui les stocke dans un data lake
    """
    def __init__(self, config=None, topic=None, data_lake_path=None):
        """
        Initialise le consommateur Kafka
        
        Args:
            config: Configuration Kafka (optionnel)
            topic: Nom du topic à consommer (optionnel)
            data_lake_path: Chemin vers le data lake (optionnel)
        """
        self.config = config or KAFKA_CONFIG.copy()
        self.topic = topic or TOPICS["web_events"]["name"]
        self.data_lake_path = data_lake_path or os.path.join(
            DATA_LAKE_CONFIG["root_path"], 
            DATA_LAKE_CONFIG["web_events_path"]
        )
        
        # Créer le répertoire du data lake s'il n'existe pas
        Path(self.data_lake_path).mkdir(parents=True, exist_ok=True)
        
        # Créer le consommateur Kafka
        self.consumer = Consumer(self.config)
        
        # Flag pour le contrôle d'exécution
        self.running = False
        
        logger.info(f"Consommateur Kafka initialisé pour le topic: {self.topic}")
        logger.info(f"Stockage des événements dans: {self.data_lake_path}")
        
    def _get_partition_path(self, date=None):
        """
        Génère le chemin du répertoire de partition basé sur la date
        
        Args:
            date: Date à utiliser (par défaut: aujourd'hui)
            
        Returns:
            Chemin complet du répertoire de partition
        """
        date_str = date or datetime.now().strftime("%Y-%m-%d")
        partition_path = os.path.join(self.data_lake_path, date_str)
        Path(partition_path).mkdir(parents=True, exist_ok=True)
        return partition_path
        
    def save_to_data_lake(self, messages, flush=True):
        """
        Sauvegarde un lot de messages dans le data lake
        
        Args:
            messages: Liste de messages à sauvegarder
            flush: Si True, écrit immédiatement sur disque
        """
        if not messages:
            return
            
        # Organiser les messages par date pour le partitionnement
        messages_by_date = {}
        for msg in messages:
            try:
                # Décodage et parsing du message
                event = json.loads(msg.value().decode('utf-8'))
                
                # Extraire la date de l'événement pour le partitionnement
                timestamp = event.get("timestamp")
                if timestamp:
                    event_date = datetime.fromisoformat(timestamp.replace('Z', '+00:00')).strftime("%Y-%m-%d")
                else:
                    event_date = datetime.now().strftime("%Y-%m-%d")
                
                if event_date not in messages_by_date:
                    messages_by_date[event_date] = []
                    
                messages_by_date[event_date].append(event)
            except Exception as e:
                logger.error(f"Erreur lors du décodage du message: {str(e)}")
        
        # Sauvegarder chaque lot dans le bon répertoire partitionné
        for date, events in messages_by_date.items():
            partition_path = self._get_partition_path(date)
            
            # Générer un nom de fichier unique basé sur timestamp
            timestamp = int(time.time() * 1000)  # Millisecondes
            filename = f"events_{timestamp}.json"
            filepath = os.path.join(partition_path, filename)
            
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(events, f, ensure_ascii=False)
                    if flush:
                        f.flush()
                        os.fsync(f.fileno())
                        
                logger.info(f"Sauvegarde de {len(events)} événements dans {filepath}")
            except Exception as e:
                logger.error(f"Erreur lors de la sauvegarde des événements: {str(e)}")
        
    def consume_batch(self, timeout=5.0, max_messages=100):
        """
        Consomme un lot de messages et les retourne
        
        Args:
            timeout: Temps maximum d'attente en secondes
            max_messages: Nombre maximum de messages à récupérer
            
        Returns:
            Liste de messages consommés
        """
        messages = []
        start_time = time.time()
        
        while len(messages) < max_messages and (time.time() - start_time) < timeout:
            msg = self.consumer.poll(1.0)  # Polling avec timeout de 1 seconde
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Fin de partition - pas d'erreur
                    continue
                else:
                    logger.error(f"Erreur Kafka: {msg.error()}")
                    break
            
            # Message valide
            messages.append(msg)
            
        return messages
        
    def start_consuming(self, batch_size=100, batch_timeout=5.0):
        """
        Démarre le processus de consommation en continu
        
        Args:
            batch_size: Taille des lots de messages
            batch_timeout: Timeout pour la récupération d'un lot
        """
        try:
            # S'abonner au topic
            self.consumer.subscribe([self.topic])
            
            # Définir le flag running
            self.running = True
            logger.info(f"Démarrage de la consommation depuis le topic {self.topic}")
            
            # Boucle principale de consommation
            while self.running:
                try:
                    # Consommer un lot de messages
                    messages = self.consume_batch(batch_timeout, batch_size)
                    
                    # Sauvegarder les messages dans le data lake
                    if messages:
                        self.save_to_data_lake(messages)
                        logger.info(f"Lot de {len(messages)} messages consommé et sauvegardé")
                    
                    # Si aucun message, attendre un peu avant de réessayer
                    elif len(messages) == 0:
                        time.sleep(1)  # Éviter de surcharger CPU en cas d'inactivité
                        
                except Exception as e:
                    logger.error(f"Erreur durant la consommation: {str(e)}")
                    # Courte pause pour éviter une boucle d'erreurs rapide
                    time.sleep(1)
                    
        except KeyboardInterrupt:
            logger.info("Interruption utilisateur reçue")
        finally:
            # Nettoyage
            self.stop_consuming()
            
    def stop_consuming(self):
        """
        Arrête le processus de consommation
        """
        self.running = False
        self.consumer.close()
        logger.info("Consommation arrêtée")

    def start_in_thread(self):
        """
        Démarre la consommation dans un thread séparé
        
        Returns:
            Thread de consommation
        """
        consumer_thread = threading.Thread(target=self.start_consuming)
        consumer_thread.daemon = True
        consumer_thread.start()
        return consumer_thread

if __name__ == "__main__":
    # Exemple d'utilisation
    consumer = KafkaWebEventsConsumer()
    
    try:
        consumer.start_consuming()
    except KeyboardInterrupt:
        logger.info("Processus interrompu par l'utilisateur")
