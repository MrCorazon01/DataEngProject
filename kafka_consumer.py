import json
import time
from kafka import KafkaConsumer

def main():
    """Consommateur Kafka simple pour vérifier les événements web"""
    print("Démarrage du consommateur d'événements web depuis Kafka...")
    
    # Plusieurs tentatives de connexion pour s'assurer que Kafka est prêt
    max_retries = 6
    retry_delay = 10
    consumer = None
    
    for attempt in range(max_retries):
        try:
            # Création du consommateur Kafka
            consumer = KafkaConsumer(
                'web_events',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='web_events_group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=10000  # Timeout après 10 secondes sans message
            )
            break  # Sortir de la boucle si la connexion est réussie
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Tentative de connexion à Kafka échouée ({attempt+1}/{max_retries}). Nouvelle tentative dans {retry_delay}s...")
                print(f"Erreur: {e}")
                time.sleep(retry_delay)
            else:
                print("Échec de connexion à Kafka après plusieurs tentatives.")
                raise
    
    try:
        print("En attente d'événements... Appuyez sur Ctrl+C pour arrêter.")
        
        # Consommation des messages
        for message in consumer:
            event = message.value
            event_type = event.get('event_type', 'inconnu')
            timestamp = event.get('timestamp', 'inconnu')
            user_id = event.get('user', {}).get('user_id', 'anonyme')
            
            print(f"Reçu: Type={event_type}, Horodatage={timestamp}, Utilisateur={user_id}")
            
            # Pour les achats, afficher des informations supplémentaires
            if event_type == 'purchase':
                purchase_info = event.get('purchase', {})
                total = purchase_info.get('total', 0)
                order_id = purchase_info.get('order_id', 'inconnu')
                print(f"  → Achat: ID={order_id}, Montant={total} XOF")
    
    except KeyboardInterrupt:
        print("\nArrêt du consommateur...")
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        if consumer:
            consumer.close()
            print("Consommateur fermé")

if __name__ == "__main__":
    main()
