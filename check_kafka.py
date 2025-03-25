import socket
import subprocess
import sys
import time
import logging
from confluent_kafka.admin import AdminClient
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_checker')

def check_docker_running():
    """Vérifier si Docker est en cours d'exécution"""
    try:
        subprocess.run(["docker", "info"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logger.info("✅ Docker est en cours d'exécution")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("❌ Docker n'est pas en cours d'exécution ou n'est pas installé")
        return False

def check_containers_status():
    """Vérifier l'état des conteneurs Kafka"""
    containers = ["zookeeper", "kafka", "schema-registry", "kafka-ui"]
    results = {}
    
    try:
        for container in containers:
            cmd = f"docker inspect --format='{{{{.State.Status}}}}' {container} 2>/dev/null || echo 'not_found'"
            status = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
            results[container] = status
            
            if status == 'running':
                logger.info(f"✅ Conteneur {container} est en cours d'exécution")
            elif status == 'not_found':
                logger.warning(f"⚠️  Conteneur {container} n'a pas été trouvé")
            else:
                logger.error(f"❌ Conteneur {container} n'est pas en cours d'exécution (statut: {status})")
                
        return results
    except Exception as e:
        logger.error(f"❌ Erreur lors de la vérification des conteneurs: {e}")
        return {}

def check_port_available(host, port):
    """Vérifier si un port est accessible"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()
        
        if result == 0:
            logger.info(f"✅ Port {port} est accessible sur {host}")
            return True
        else:
            logger.error(f"❌ Port {port} n'est pas accessible sur {host}")
            return False
    except Exception as e:
        logger.error(f"❌ Erreur lors de la vérification du port {port}: {e}")
        return False

def check_kafka_connection():
    """Vérifier la connexion à Kafka avec Confluent Kafka"""
    try:
        config = {
            'bootstrap.servers': 'localhost:29092',
            'client.id': 'kafka-checker',
            'socket.timeout.ms': 10000,
            'request.timeout.ms': 20000
        }
        
        admin_client = AdminClient(config)
        metadata = admin_client.list_topics(timeout=10)
        
        brokers = metadata.brokers
        if brokers:
            logger.info(f"✅ Connexion à Kafka réussie! Brokers disponibles: {len(brokers)}")
            for broker_id, (host, port) in brokers.items():
                logger.info(f"   - Broker {broker_id}: {host}:{port}")
            return True
        else:
            logger.warning("⚠️  Connexion à Kafka établie mais aucun broker trouvé")
            return False
    except KafkaException as e:
        logger.error(f"❌ Erreur de connexion à Kafka: {e}")
        return False

def test_producer_consumer():
    """Tester un cycle complet de production/consommation"""
    topic = "kafka-test-topic"
    config = {
        'bootstrap.servers': 'localhost:29092',
        'socket.timeout.ms': 10000,
        'request.timeout.ms': 20000
    }
    
    # Tenter de produire un message
    try:
        producer = Producer({**config, 'client.id': 'test-producer'})
        producer.produce(topic, value="Test message".encode('utf-8'))
        producer.flush(timeout=10)
        logger.info(f"✅ Message produit avec succès sur le topic {topic}")
        producer_success = True
    except Exception as e:
        logger.error(f"❌ Échec de production sur {topic}: {e}")
        producer_success = False
    
    # Tenter de consommer le message
    if producer_success:
        try:
            consumer = Consumer({
                **config,
                'group.id': 'test-consumer',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True
            })
            consumer.subscribe([topic])
            
            msg = consumer.poll(timeout=10)
            if msg:
                if msg.error():
                    logger.error(f"❌ Erreur lors de la consommation: {msg.error()}")
                    return False
                else:
                    logger.info(f"✅ Message consommé avec succès du topic {topic}: {msg.value().decode('utf-8')}")
                    consumer.close()
                    return True
            else:
                logger.warning(f"⚠️  Aucun message reçu du topic {topic} après 10 secondes")
                consumer.close()
                return False
        except Exception as e:
            logger.error(f"❌ Échec de consommation depuis {topic}: {e}")
            return False
    return False

def run_diagnostics():
    """Exécuter tous les tests de diagnostic"""
    print("🔍 Démarrage des diagnostics Kafka...\n")
    
    # Vérifier Docker
    if not check_docker_running():
        print("\n❌ Docker n'est pas en cours d'exécution. Veuillez démarrer Docker et réessayer.")
        return False
    
    # Vérifier l'état des conteneurs
    container_statuses = check_containers_status()
    
    # Vérifier les ports critiques
    print("\n🔌 Vérification des ports...")
    ports_to_check = [
        ('localhost', 2181),  # Zookeeper
        ('localhost', 9092),  # Kafka interne
        ('localhost', 29092), # Kafka externe
        ('localhost', 8081),  # Schema Registry
        ('localhost', 8080)   # Kafka UI
    ]
    
    port_results = {}
    for host, port in ports_to_check:
        port_results[(host, port)] = check_port_available(host, port)
    
    # Vérifier la connexion à Kafka
    print("\n🔄 Test de connexion à Kafka...")
    kafka_connected = check_kafka_connection()
    
    # Tester la production et la consommation
    print("\n📤 Test production/consommation...")
    producer_consumer_works = test_producer_consumer()
    
    # Résumé
    print("\n📋 RÉSUMÉ DES DIAGNOSTICS:")
    print("------------------------")
    print(f"Docker en cours d'exécution: {'✅' if check_docker_running() else '❌'}")
    
    print("\nÉtat des conteneurs:")
    for container, status in container_statuses.items():
        status_icon = '✅' if status == 'running' else '❌'
        print(f"- {container}: {status_icon} {status}")
    
    print("\nAccessibilité des ports:")
    for (host, port), available in port_results.items():
        status_icon = '✅' if available else '❌'
        print(f"- {host}:{port}: {status_icon}")
    
    print(f"\nConnexion à Kafka: {'✅' if kafka_connected else '❌'}")
    print(f"Test production/consommation: {'✅' if producer_consumer_works else '❌'}")
    
    # Suggestions en cas de problème
    if not all(status == 'running' for status in container_statuses.values()):
        print("\n🛠️ SUGGESTION: Certains conteneurs ne sont pas en cours d'exécution.")
        print("   Exécutez: docker-compose down && docker-compose up -d")
        
    if not kafka_connected:
        print("\n🛠️ SUGGESTION: Problème de connexion à Kafka.")
        print("   - Vérifiez que le port 29092 est correctement exposé")
        print("   - Vérifiez les logs avec: docker logs kafka")
        print("   - Redémarrez les conteneurs: docker-compose restart kafka")
        
    if kafka_connected and not producer_consumer_works:
        print("\n🛠️ SUGGESTION: Problème avec la production/consommation de messages.")
        print("   - Vérifiez les permissions sur les topics")
        print("   - Vérifiez les logs avec: docker logs kafka")
    
    return all([
        all(status == 'running' for status in container_statuses.values()),
        all(port_results.values()),
        kafka_connected,
        producer_consumer_works
    ])

if __name__ == "__main__":
    if run_diagnostics():
        print("\n✅ VERDICT: Tout semble fonctionner correctement! 🎉")
        sys.exit(0)
    else:
        print("\n⚠️ VERDICT: Des problèmes ont été détectés. Veuillez consulter les suggestions ci-dessus.")
        sys.exit(1)
