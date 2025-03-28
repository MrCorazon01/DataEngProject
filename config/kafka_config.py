import os

# Configuration Kafka
KAFKA_CONFIG = {
    # Utiliser le port 29092 qui est exposé par Docker Compose pour les clients externes
    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
    "client.id": "biocosmetics-client",
    "auto.offset.reset": "earliest",
    "group.id": "biocosmetics-consumer-group",
    # Ajouter timeout pour éviter les blocages
    "socket.timeout.ms": 10000,
    "request.timeout.ms": 20000,
    "metadata.request.timeout.ms": 20000
}

# Configuration des topics
TOPICS = {
    "web_events": {
        "name": "web-events",
        "partitions": 5,
        "replication_factor": 1,
        "retention_ms": 604800000,  # 7 jours
        "cleanup_policy": "delete"
    },
    "crm_events": {
        "name": "crm-events",
        "partitions": 3,
        "replication_factor": 1,
        "retention_ms": 2592000000,  # 30 jours
        "cleanup_policy": "compact"
    },
    "marketing_events": {
        "name": "marketing-events",
        "partitions": 3,
        "replication_factor": 1,
        "retention_ms": 2592000000,  # 30 jours
        "cleanup_policy": "delete"
    }
}

# Configuration Schema Registry
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# Configuration de sauvegarde vers Data Lake
DATA_LAKE_CONFIG = {
    "root_path": os.environ.get("DATA_LAKE_PATH", "/tmp/datalake"),
    "web_events_path": "web-events/raw",
    "crm_events_path": "crm/raw",
    "marketing_events_path": "advertising/raw"
}
