import json
import time
import argparse
from kafka import KafkaConsumer

def get_consumer(topic):
    """Crée un consommateur Kafka pour un topic spécifique"""
    # Plusieurs tentatives de connexion pour s'assurer que Kafka est prêt
    max_retries = 6
    retry_delay = 10
    
    for attempt in range(max_retries):
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=f'{topic}_group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=10000  # Timeout après 10 secondes sans message
            )
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Tentative de connexion à Kafka échouée ({attempt+1}/{max_retries}). Nouvelle tentative dans {retry_delay}s...")
                print(f"Erreur: {e}")
                time.sleep(retry_delay)
            else:
                print("Échec de connexion à Kafka après plusieurs tentatives.")
                raise

def display_web_event(event):
    """Affiche un événement web de façon formatée"""
    event_type = event.get('event_type', 'inconnu')
    timestamp = event.get('timestamp', 'inconnu')
    user_id = event.get('user', {}).get('user_id', 'anonyme')
    
    print(f"Web Event: Type={event_type}, Horodatage={timestamp}, Utilisateur={user_id}")
    
    # Pour les achats, afficher des informations supplémentaires
    if event_type == 'purchase':
        purchase_info = event.get('purchase', {})
        total = purchase_info.get('total', 0)
        order_id = purchase_info.get('order_id', 'inconnu')
        print(f"  → Achat: ID={order_id}, Montant={total} XOF")

def display_campaign(campaign):
    """Affiche une campagne publicitaire de façon formatée"""
    campaign_id = campaign.get('campaign_id', 'inconnu')
    name = campaign.get('name', 'inconnu')
    platform = campaign.get('platform', 'inconnu')
    status = campaign.get('status', 'inconnu')
    budget = campaign.get('total_budget', 0)
    
    print(f"Campagne: ID={campaign_id}, Nom={name}")
    print(f"  → Plateforme={platform}, Statut={status}, Budget={budget} XOF")
    
    # Afficher les produits promus
    products = campaign.get('promoted_products', [])
    if products:
        print(f"  → Produits: {', '.join(products)}")

def display_performance(perf):
    """Affiche une performance publicitaire de façon formatée"""
    campaign_id = perf.get('campaign_id', 'inconnu')
    date = perf.get('date', 'inconnu')
    platform = perf.get('platform', 'inconnu')
    spend = perf.get('spend', 0)
    impressions = perf.get('impressions', 0)
    clicks = perf.get('clicks', 0)
    conversions = perf.get('conversions', 0)
    
    print(f"Performance: ID={campaign_id}, Date={date}, Plateforme={platform}")
    print(f"  → Dépenses={spend} XOF, Impressions={impressions}, Clics={clicks}, Conversions={conversions}")
    
    roas = perf.get('roas', 0)
    if roas > 0:
        print(f"  → ROAS={roas}x, Valeur conversions={perf.get('conversion_value', 0)} XOF")

def display_client(client):
    """Affiche un client CRM de façon formatée"""
    client_id = client.get('client_id', 'inconnu')
    nom = client.get('nom', '')
    prenom = client.get('prenom', '')
    segment = client.get('segment', 'inconnu')
    ville = client.get('adresse', {}).get('ville', 'inconnu') if 'adresse' in client else 'inconnu'
    valeur = client.get('valeur_client', 0)
    
    print(f"Client: ID={client_id}, Nom={prenom} {nom}, Segment={segment}")
    print(f"  → Ville={ville}, Valeur={valeur} XOF")
    
    if 'update_timestamp' in client:
        print(f"  → [MISE À JOUR] Timestamp={client['update_timestamp']}")

def display_interaction(interaction):
    """Affiche une interaction client de façon formatée"""
    interaction_id = interaction.get('interaction_id', 'inconnu')
    client_id = interaction.get('client_id', 'inconnu')
    date = interaction.get('date', 'inconnu')
    type_interaction = interaction.get('type', 'inconnu')
    statut = interaction.get('statut', 'inconnu')
    
    print(f"Interaction: ID={interaction_id}, Client={client_id}, Date={date}")
    print(f"  → Type={type_interaction}, Statut={statut}")
    
    satisfaction = interaction.get('satisfaction')
    if satisfaction:
        print(f"  → Satisfaction: {satisfaction}/5")

def main():
    """Fonction principale du consommateur spécialisé"""
    parser = argparse.ArgumentParser(description='Consommateur Kafka spécialisé par type de données')
    parser.add_argument('topic', choices=['web_events', 'ad_campaigns', 'ad_performances', 'crm_clients', 'crm_interactions'],
                        help='Topic Kafka à consommer')
    args = parser.parse_args()
    
    topic = args.topic
    
    print(f"Démarrage du consommateur pour le topic '{topic}'...")
    
    try:
        # Créer le consommateur approprié
        consumer = get_consumer(topic)
        
        print("En attente de messages... Appuyez sur Ctrl+C pour arrêter.")
        
        # Consommation des messages avec affichage spécialisé
        for message in consumer:
            data = message.value
            
            print("\n" + "-" * 50)
            
            if topic == 'web_events':
                display_web_event(data)
            elif topic == 'ad_campaigns':
                display_campaign(data)
            elif topic == 'ad_performances':
                display_performance(data)
            elif topic == 'crm_clients':
                display_client(data)
            elif topic == 'crm_interactions':
                display_interaction(data)
                
            print("-" * 50)
    
    except KeyboardInterrupt:
        print("\nArrêt du consommateur...")
    except Exception as e:
        print(f"Erreur: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
            print("Consommateur fermé")

if __name__ == "__main__":
    main()
