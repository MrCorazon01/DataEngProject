import sys
import os
import logging
from confluent_kafka.admin import AdminClient, NewTopic

# Ajouter le répertoire parent au path pour pouvoir importer les configurations
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.kafka_config import KAFKA_CONFIG, TOPICS

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_setup')

class KafkaSetup:
    def __init__(self, config=None):
        """
        Initialise l'utilitaire de configuration Kafka
        
        Args:
            config: Configuration Kafka (optionnel)
        """
        self.config = config or KAFKA_CONFIG
        self.admin_client = AdminClient(self.config)
        
    def create_topics(self, topics=None):
        """
        Crée les topics Kafka requis
        
        Args:
            topics: Dict de configurations des topics (optionnel)
        
        Returns:
            Dict des résultats de création des topics
        """
        topics_config = topics or TOPICS
        
        # Vérifier les topics existants
        metadata = self.admin_client.list_topics(timeout=10)
        existing_topics = metadata.topics
        
        # Préparer les nouveaux topics à créer
        new_topics = []
        
        for key, topic_conf in topics_config.items():
            topic_name = topic_conf["name"]
            
            if topic_name in existing_topics:
                logger.info(f"Le topic '{topic_name}' existe déjà")
                continue
                
            # Configuration du topic
            config = {
                "retention.ms": str(topic_conf.get("retention_ms", 604800000)),  # 7 jours par défaut
                "cleanup.policy": topic_conf.get("cleanup_policy", "delete")
            }
            
            # Créer une configuration NewTopic
            new_topic = NewTopic(
                topic_name,
                num_partitions=topic_conf.get("partitions", 3),
                replication_factor=topic_conf.get("replication_factor", 1),
                config=config
            )
            
            new_topics.append(new_topic)
        
        if not new_topics:
            logger.info("Aucun nouveau topic à créer")
            return {}
            
        # Créer les topics
        logger.info(f"Création de {len(new_topics)} topics: {', '.join(t.topic for t in new_topics)}")
        
        # Envoyer la requête de création
        fs = self.admin_client.create_topics(new_topics)
        
        # Attendre et récupérer les résultats
        results = {}
        for topic, f in fs.items():
            try:
                f.result()  # Le résultat est None en cas de succès
                logger.info(f"Topic '{topic}' créé avec succès")
                results[topic] = True
            except Exception as e:
                logger.error(f"Échec de création du topic '{topic}': {str(e)}")
                results[topic] = False
                
        return results
    
    def list_topics(self):
        """Liste les topics existants"""
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            topics = list(metadata.topics.keys())
            logger.info(f"Topics existants ({len(topics)}): {', '.join(topics)}")
            return topics
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des topics: {str(e)}")
            return []
    
if __name__ == "__main__":
    # Exemple d'utilisation
    setup = KafkaSetup()
    
    # Lister les topics existants
    setup.list_topics()
    
    # Créer les topics nécessaires
    setup.create_topics()
