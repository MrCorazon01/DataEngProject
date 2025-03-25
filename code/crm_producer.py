import time
import json
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from data_generation.crm_data_generator import CRMDataGenerator

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
    """Fonction principale pour générer et envoyer des données CRM à Kafka"""
    print("Démarrage du producteur de données CRM pour Kafka...")
    
    try:
        # Création du générateur de données CRM
        generator = CRMDataGenerator()
        
        # Création du producteur Kafka
        producer = create_producer()
        
        # Topics Kafka
        client_topic = 'crm_clients'
        interaction_topic = 'crm_interactions'
        
        # Compteur d'événements
        clients_count = 0
        interactions_count = 0
        
        print("Génération des données initiales...")
        
        # Générer un ensemble initial de données
        crm_data = generator.generate_crm_data(num_clients=50, min_interactions=2, max_interactions=8)
        
        # Liste pour suivre les clients déjà envoyés
        sent_clients = []
        
        # Envoyer les clients
        for client in crm_data["clients"]:
            producer.send(client_topic, value=client)
            clients_count += 1
            sent_clients.append(client["client_id"])
            
        # Envoyer les interactions
        for interaction in crm_data["interactions"]:
            producer.send(interaction_topic, value=interaction)
            interactions_count += 1
            
        print(f"Données initiales envoyées: {clients_count} clients et {interactions_count} interactions")
        
        # Génération périodique de nouvelles données
        while True:
            print("\nMise à jour des données CRM...")
            
            # 1. Possibilité d'ajouter de nouveaux clients (15% de chance)
            if random.random() < 0.15:
                num_new_clients = random.randint(1, 5)
                print(f"Ajout de {num_new_clients} nouveaux clients")
                
                for _ in range(num_new_clients):
                    new_client = generator.generate_client()
                    producer.send(client_topic, value=new_client)
                    clients_count += 1
                    sent_clients.append(new_client["client_id"])
                    
                    # Générer quelques interactions pour ce nouveau client
                    for _ in range(random.randint(1, 3)):
                        new_interaction = generator.generate_interaction(new_client["client_id"])
                        producer.send(interaction_topic, value=new_interaction)
                        interactions_count += 1
            
            # 2. Ajouter des interactions pour les clients existants
            num_interactions = random.randint(5, 15)
            print(f"Ajout de {num_interactions} nouvelles interactions")
            
            for _ in range(num_interactions):
                # Sélectionner un client aléatoire parmi ceux existants
                if sent_clients:
                    client_id = random.choice(sent_clients)
                    
                    # Générer une interaction récente
                    days_ago = random.randint(0, 2)
                    interaction_date = datetime.now() - timedelta(days=days_ago, 
                                                                 hours=random.randint(0, 23), 
                                                                 minutes=random.randint(0, 59))
                    
                    new_interaction = generator.generate_interaction(client_id, interaction_date)
                    producer.send(interaction_topic, value=new_interaction)
                    interactions_count += 1
            
            # 3. Mettre à jour le statut de certains clients (10% de chance par client)
            clients_to_update = []
            for client_id in sent_clients:
                if random.random() < 0.1:
                    clients_to_update.append(client_id)
            
            if clients_to_update:
                print(f"Mise à jour de {len(clients_to_update)} profils clients")
                
                for client_id in clients_to_update:
                    # Simuler une mise à jour simple
                    update = {
                        "client_id": client_id,
                        "segment": random.choice(list(generator.segments.keys())),
                        "statut_fidelite": random.choice(generator.fidelity_status),
                        "dernier_achat": datetime.now().isoformat(),
                        "update_timestamp": datetime.now().isoformat()
                    }
                    producer.send(client_topic, value=update)
            
            print(f"Total: {clients_count} clients et {interactions_count} interactions envoyés")
            
            # Attendre entre 30 secondes et 1 minute avant la prochaine mise à jour
            wait_time = random.uniform(30, 60)
            print(f"Prochaine mise à jour dans {int(wait_time)} secondes...")
            time.sleep(wait_time)
            
    except KeyboardInterrupt:
        print("\nArrêt du producteur de données CRM...")
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        if 'producer' in locals():
            producer.close()
            print("Producteur fermé")

if __name__ == "__main__":
    main()
