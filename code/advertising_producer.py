import time
import json
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from data_generation.advertising_data_generator import AdvertisingDataGenerator

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
    """Fonction principale pour générer et envoyer des données publicitaires à Kafka"""
    print("Démarrage du producteur de données publicitaires pour Kafka...")
    
    try:
        # Création du générateur de données publicitaires
        generator = AdvertisingDataGenerator()
        
        # Création du producteur Kafka
        producer = create_producer()
        
        # Topics Kafka
        campaign_topic = 'ad_campaigns'
        performance_topic = 'ad_performances'
        
        # Compteur d'événements
        campaigns_count = 0
        performances_count = 0
        
        print("Génération des données initiales...")
        
        # Générer un ensemble initial de données
        ad_data = generator.generate_advertising_data(num_campaigns=5)
        
        # Envoyer les campagnes
        for campaign in ad_data["campaigns"]:
            producer.send(campaign_topic, value=campaign)
            campaigns_count += 1
            
        # Envoyer les performances
        for performance in ad_data["daily_performances"]:
            producer.send(performance_topic, value=performance)
            performances_count += 1
            
        print(f"Données initiales envoyées: {campaigns_count} campagnes et {performances_count} rapports de performance")
        
        # Génération périodique de nouvelles données
        while True:
            print("\nMise à jour des données publicitaires...")
            
            # 1. Possibilité d'ajouter une nouvelle campagne (20% de chance)
            if random.random() < 0.2:
                new_campaign = generator.generate_campaign()
                producer.send(campaign_topic, value=new_campaign)
                campaigns_count += 1
                print(f"Nouvelle campagne ajoutée: {new_campaign['name']}")
                
                # Générer quelques performances pour cette campagne
                start_date = datetime.fromisoformat(new_campaign["start_date"])
                end_date = datetime.now()
                if end_date > start_date:
                    current_date = start_date
                    while current_date <= end_date:
                        daily_perf = generator.generate_daily_performance(new_campaign, current_date)
                        if daily_perf:
                            producer.send(performance_topic, value=daily_perf)
                            performances_count += 1
                        current_date += timedelta(days=1)
            
            # 2. Ajouter les performances du jour pour les campagnes existantes
            today = datetime.now()
            for campaign in ad_data["campaigns"]:
                campaign_end = datetime.fromisoformat(campaign["end_date"])
                if today <= campaign_end:
                    daily_perf = generator.generate_daily_performance(campaign, today)
                    if daily_perf:
                        producer.send(performance_topic, value=daily_perf)
                        performances_count += 1
            
            print(f"Total: {campaigns_count} campagnes et {performances_count} rapports de performance envoyés")
            
            # Attendre entre 1 et 2 minutes avant la prochaine mise à jour
            wait_time = random.uniform(60, 120)
            print(f"Prochaine mise à jour dans {int(wait_time)} secondes...")
            time.sleep(wait_time)
            
    except KeyboardInterrupt:
        print("\nArrêt du producteur de données publicitaires...")
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        if 'producer' in locals():
            producer.close()
            print("Producteur fermé")

if __name__ == "__main__":
    main()
