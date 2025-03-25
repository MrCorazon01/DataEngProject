#!/bin/bash

echo "=== Configuration et lancement du système Kafka avec Docker pour l'ingestion d'événements web ==="
echo ""

# Vérifier si Docker est installé
if ! command -v docker &> /dev/null; then
    echo "Docker n'est pas installé. Veuillez l'installer depuis: https://docs.docker.com/get-docker/"
    exit 1
fi

# Vérifier si Docker Compose est installé
if ! command -v docker-compose &> /dev/null; then
    echo "Docker Compose n'est pas installé. Veuillez l'installer depuis: https://docs.docker.com/compose/install/"
    exit 1
fi

# Installer les dépendances Python requises
echo "Installation des dépendances Python..."
pip install -r requirements.txt
echo ""

# Lancer Kafka et Zookeeper avec Docker Compose
echo "Démarrage de Kafka et Zookeeper avec Docker Compose..."
docker-compose up -d
echo ""

# Attendre que les services soient prêts
echo "Attente du démarrage des services..."
sleep 10

# Vérifier que Kafka est prêt
echo "Vérification que Kafka est prêt..."
MAX_RETRIES=12
RETRY_INTERVAL=10
counter=0

while ! docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list &> /dev/null; do
    counter=$((counter+1))
    if [ $counter -eq $MAX_RETRIES ]; then
        echo "Échec du démarrage de Kafka après $MAX_RETRIES tentatives."
        exit 1
    fi
    echo "Kafka n'est pas encore prêt. Nouvelle tentative dans $RETRY_INTERVAL secondes... ($counter/$MAX_RETRIES)"
    sleep $RETRY_INTERVAL
done

echo "Kafka est prêt!"
echo ""

# Créer le topic si nécessaire
echo "Création du topic 'web_events' s'il n'existe pas déjà..."
docker exec kafka kafka-topics --create --topic web_events --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1 --if-not-exists
echo ""

# Afficher des informations utiles
echo "=== Configuration terminée ! ==="
echo ""
echo "→ L'interface Kafka UI est disponible sur: http://localhost:8080"
echo ""
echo "=== Instructions pour exécuter l'exemple ==="
echo "1. Dans un terminal, démarrez le producteur:"
echo "   python kafka_producer.py"
echo ""
echo "2. Dans un autre terminal, démarrez le consommateur pour vérifier la réception des événements:"
echo "   python kafka_consumer.py"
echo ""
echo "3. Pour arrêter les services Docker quand vous avez terminé:"
echo "   docker-compose down"
echo ""
echo "Appuyez sur Ctrl+C pour arrêter les scripts à tout moment."
