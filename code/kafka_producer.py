import time
import json
import random
from kafka import KafkaProducer
from data_generation.web_events_generator import WebEventGenerator

def json_serializer(data):
    """Sérialise les données en JSON"""
    return json.dumps(data).encode('utf-8')

def create_producer():
    """Crée une instance du producteur Kafka"""
    # Plusieurs tentatives de connexion pour s'assurer que Kafka est prêt
    max_retries = 6
    retry_delay = 10
    
    for attempt in range(max_retries):
        try:
            return KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=json_serializer
            )
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Tentative de connexion à Kafka échouée ({attempt+1}/{max_retries}). Nouvelle tentative dans {retry_delay}s...")
                print(f"Erreur: {e}")
                time.sleep(retry_delay)
            else:
                print("Échec de connexion à Kafka après plusieurs tentatives.")
                raise

def main():
    """Fonction principale pour générer et envoyer des événements web à Kafka"""
    print("Démarrage du producteur d'événements web pour Kafka...")
    
    try:
        # Création du générateur d'événements web
        generator = WebEventGenerator()
        
        # Création du producteur Kafka
        producer = create_producer()
        
        # Nom du topic Kafka
        topic = 'web_events'
        
        # Compteur d'événements
        count = 0
        
        # Production continue d'événements
        while True:
            # Génération aléatoire: soit un événement unique, soit une séquence de parcours utilisateur
            if random.random() < 0.3:
                # Génération d'un parcours utilisateur (entre 2 et 6 événements)
                events = generator.generate_user_journey(random.randint(2, 6))
                print(f"Envoi d'un parcours utilisateur de {len(events)} événements")
                
                for event in events:
                    producer.send(topic, value=event)
                    count += 1
            else:
                # Génération d'un seul événement
                event = generator.generate_event()
                producer.send(topic, value=event)
                print(f"Événement envoyé: {event['event_type']}")
                count += 1
            
            # Affichage du nombre total d'événements envoyés
            print(f"Total des événements envoyés: {count}")
            
            # Attente aléatoire entre 0.5 et 2 secondes
            time.sleep(random.uniform(0.5, 2))
            
    except KeyboardInterrupt:
        print("\nArrêt du producteur...")
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        if 'producer' in locals():
            producer.close()
            print("Producteur fermé")

if __name__ == "__main__":
    main()
