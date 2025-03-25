import os
import time
import argparse
import logging
import threading
import subprocess
from pathlib import Path
import socket

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pipeline_starter')

def check_port_available(host, port):
    """Vérifie si un port est disponible sur un hôte donné"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((host, port))
        s.shutdown(socket.SHUT_RDWR)
        return True
    except:
        return False
    finally:
        s.close()

def wait_for_kafka(max_retries=10, retry_interval=5):
    """Attend que Kafka soit disponible"""
    logger.info(f"Vérification de la disponibilité de Kafka...")
    retries = 0
    while retries < max_retries:
        if check_port_available('localhost', 29092):
            logger.info("Kafka est disponible.")
            return True
        logger.info(f"Kafka n'est pas encore disponible. Attente ({retries+1}/{max_retries})...")
        retries += 1
        time.sleep(retry_interval)
    logger.error(f"Kafka n'est pas disponible après {max_retries} tentatives.")
    return False

def start_kafka_infrastructure():
    """Démarre l'infrastructure Kafka avec Docker Compose"""
    logger.info("Démarrage de l'infrastructure Kafka...")
    try:
        # Vérifier si Docker est en cours d'exécution
        try:
            subprocess.run(["docker", "info"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError:
            logger.error("Docker n'est pas démarré. Veuillez démarrer Docker avant de lancer le pipeline.")
            return False
            
        # Démarrer les conteneurs
        subprocess.run(["docker-compose", "down"], check=False)  # Arrêter d'abord les conteneurs existants
        subprocess.run(["docker-compose", "up", "-d"], check=True)
        logger.info("Infrastructure Kafka démarrée avec succès. Attente de la disponibilité des services...")
        
        # Attendre que Kafka soit prêt
        if not wait_for_kafka(max_retries=15, retry_interval=5):
            logger.error("Timeout en attendant le démarrage complet de Kafka")
            return False
        
        # Attente supplémentaire pour que Schema Registry soit prêt
        logger.info("Attente supplémentaire pour que tous les services soient prêts...")
        time.sleep(10)
        
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Erreur lors du démarrage de l'infrastructure Kafka: {e}")
        return False

def setup_kafka_topics():
    """Configure les topics Kafka nécessaires"""
    logger.info("Configuration des topics Kafka...")
    try:
        from data_ingestion.kafka_setup import KafkaSetup
        
        # Configuration spécifique avec le bon port
        config = {
            "bootstrap.servers": "localhost:29092",  # Utiliser le port exposé par Docker
            "client.id": "kafka-setup-client"
        }
        
        setup = KafkaSetup(config)
        setup.list_topics()  # Vérifier la connexion
        setup.create_topics()
        logger.info("Topics Kafka configurés avec succès")
        return True
    except Exception as e:
        logger.error(f"Erreur lors de la configuration des topics Kafka: {e}")
        logger.exception("Détails de l'erreur:")
        return False

def start_web_events_producer(events_per_second=10, duration_seconds=None):
    """Démarre le producteur d'événements web"""
    logger.info(f"Démarrage du producteur d'événements web (taux: {events_per_second}/sec)...")
    try:
        from data_ingestion.kafka_producer import KafkaWebEventsProducer
        
        producer = KafkaWebEventsProducer(
            bootstrap_servers="localhost:29092",
            topic_name="web-events"
        )
        
        # Démarrer le producteur dans un thread
        producer_thread = threading.Thread(
            target=producer.simulate_traffic,
            args=(events_per_second, duration_seconds)
        )
        producer_thread.daemon = True
        producer_thread.start()
        
        logger.info("Producteur d'événements web démarré dans un thread séparé")
        return producer_thread
    except Exception as e:
        logger.error(f"Erreur lors du démarrage du producteur d'événements web: {e}")
        return None

def start_web_events_consumer():
    """Démarre le consommateur d'événements web"""
    logger.info("Démarrage du consommateur d'événements web...")
    try:
        from data_ingestion.kafka_consumer import KafkaWebEventsConsumer
        
        # Configuration spécifique pour le consommateur
        config = {
            "bootstrap.servers": "localhost:29092",
            "auto.offset.reset": "earliest",
            "group.id": "web-events-consumer",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000
        }
        
        consumer = KafkaWebEventsConsumer(
            config=config,
            topic="web-events",
            data_lake_path="./datalake/web-events/raw"
        )
        
        # Démarrer le consommateur dans un thread
        consumer_thread = threading.Thread(target=consumer.start_consuming)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        logger.info("Consommateur d'événements web démarré dans un thread séparé")
        return consumer_thread
    except Exception as e:
        logger.error(f"Erreur lors du démarrage du consommateur d'événements web: {e}")
        return None

def generate_crm_data():
    """Génère et sauvegarde les données CRM batch"""
    logger.info("Génération des données CRM...")
    try:
        from data_generation.crm_data_generator import CRMDataGenerator
        import json
        
        generator = CRMDataGenerator()
        data = generator.generate_crm_data(num_clients=50, min_interactions=2, max_interactions=8)
        
        # Créer le répertoire de sortie
        output_dir = Path("./datalake/crm/raw")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Sauvegarder les données
        timestamp = int(time.time())
        output_file = output_dir / f"crm_data_{timestamp}.json"
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Données CRM générées et sauvegardées dans {output_file}")
        return str(output_file)
    except Exception as e:
        logger.error(f"Erreur lors de la génération des données CRM: {e}")
        return None

def generate_advertising_data():
    """Génère et sauvegarde les données publicitaires batch"""
    logger.info("Génération des données publicitaires...")
    try:
        from data_generation.advertising_data_generator import AdvertisingDataGenerator
        import json
        
        generator = AdvertisingDataGenerator()
        data = generator.generate_advertising_data(num_campaigns=10)
        
        # Créer le répertoire de sortie
        output_dir = Path("./datalake/advertising/raw")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Sauvegarder les données
        timestamp = int(time.time())
        output_file = output_dir / f"ad_data_{timestamp}.json"
        
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Données publicitaires générées et sauvegardées dans {output_file}")
        return str(output_file)
    except Exception as e:
        logger.error(f"Erreur lors de la génération des données publicitaires: {e}")
        return None

def generate_aligned_data():
    """Génère et sauvegarde des données alignées (web, CRM, publicité)"""
    logger.info("Génération de données alignées...")
    try:
        from data_integration.data_aligner import DataAligner
        
        aligner = DataAligner(seed=42)
        
        # Créer le répertoire de sortie
        output_dir = Path("./datalake/aligned_data")
        
        # Sauvegarder les données alignées
        aligner.save_datasets(output_dir=str(output_dir), formatted=True)
        
        logger.info(f"Données alignées générées et sauvegardées dans {output_dir}")
        return str(output_dir)
    except Exception as e:
        logger.error(f"Erreur lors de la génération des données alignées: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Démarre le pipeline de données")
    parser.add_argument("--kafka", action="store_true", help="Démarre l'infrastructure Kafka")
    parser.add_argument("--producer", action="store_true", help="Démarre le producteur d'événements web")
    parser.add_argument("--consumer", action="store_true", help="Démarre le consommateur d'événements web")
    parser.add_argument("--crm", action="store_true", help="Génère les données CRM")
    parser.add_argument("--ads", action="store_true", help="Génère les données publicitaires")
    parser.add_argument("--aligned", action="store_true", help="Génère des données alignées")
    parser.add_argument("--all", action="store_true", help="Démarre tous les composants")
    parser.add_argument("--rate", type=int, default=10, help="Taux d'événements par seconde")
    parser.add_argument("--duration", type=int, default=None, help="Durée de génération des événements (secondes)")
    
    args = parser.parse_args()
    
    # Si aucune option n'est spécifiée, afficher l'aide
    if not any(vars(args).values()):
        parser.print_help()
        return
        
    # Démarrer les composants sélectionnés
    threads = []
    
    if args.kafka or args.all:
        kafka_success = start_kafka_infrastructure()
        if kafka_success:
            setup_result = setup_kafka_topics()
            if not setup_result:
                logger.warning("Échec de la configuration des topics. Tentative de continuer quand même...")
        else:
            logger.error("Échec du démarrage de Kafka. Arrêt du pipeline.")
            return
        
    if args.producer or args.all:
        producer_thread = start_web_events_producer(
            events_per_second=args.rate,
            duration_seconds=args.duration
        )
        if producer_thread:
            threads.append(producer_thread)
            
    if args.consumer or args.all:
        consumer_thread = start_web_events_consumer()
        if consumer_thread:
            threads.append(consumer_thread)
            
    if args.crm or args.all:
        generate_crm_data()
        
    if args.ads or args.all:
        generate_advertising_data()
        
    if args.aligned or args.all:
        generate_aligned_data()
        
    # Attendre que les threads se terminent (ou interruption par utilisateur)
    try:
        for t in threads:
            while t.is_alive():
                t.join(1)
    except KeyboardInterrupt:
        logger.info("Interruption par l'utilisateur, arrêt...")

if __name__ == "__main__":
    main()
