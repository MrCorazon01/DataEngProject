import sys
import os
import json
import random
from datetime import datetime, timedelta
import logging

# Ajouter le répertoire parent au path pour pouvoir importer les générateurs
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_generation.web_events_generator import WebEventGenerator
from data_generation.crm_data_generator import CRMDataGenerator
from data_generation.advertising_data_generator import AdvertisingDataGenerator

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_aligner')

class DataAligner:
    """
    Classe responsable de l'alignement des données entre les différentes sources:
    - Événements web
    - Données CRM
    - Données publicitaires
    
    Cette classe permet de générer des données cohérentes entre elles, en assurant
    que les identifiants clients et campagnes sont correctement liés entre les différentes sources.
    """
    
    def __init__(self, seed=None):
        """
        Initialise l'aligneur de données
        
        Args:
            seed: Graine pour la reproductibilité des données générées
        """
        if seed:
            random.seed(seed)
            
        logger.info("Initialisation de l'aligneur de données")
        
        # Initialiser les générateurs
        self.crm_generator = CRMDataGenerator()
        self.ad_generator = AdvertisingDataGenerator()
        
        # Générer les données de base
        self.crm_data = self.crm_generator.generate_crm_data(num_clients=50)
        self.ad_data = self.ad_generator.generate_advertising_data(num_campaigns=10)
        
        # Extraire les identifiants pour l'alignement
        self.client_ids = [client["client_id"] for client in self.crm_data["clients"]]
        self.campaign_ids = [campaign["campaign_id"] for campaign in self.ad_data["campaigns"]]
        
        # Créer des dictionnaires pour des recherches efficaces
        self.client_dict = {client["client_id"]: client for client in self.crm_data["clients"]}
        self.campaign_dict = {camp["campaign_id"]: camp for camp in self.ad_data["campaigns"]}
        
        # Initialiser le générateur web avec les références aux autres sources
        self.web_generator = WebEventGenerator(
            crm_ids=self.client_ids, 
            campaign_ids=self.campaign_ids
        )
        
        # Tableau de correspondance des sessions utilisateurs (session_id -> client_id)
        self.session_user_mapping = {}
        
        logger.info(f"Données CRM générées pour {len(self.client_ids)} clients")
        logger.info(f"Données publicitaires générées pour {len(self.campaign_ids)} campagnes")
    
    def generate_aligned_web_events(self, count=100):
        """
        Génère des événements web alignés avec les données CRM et publicitaires
        
        Args:
            count: Nombre d'événements à générer
            
        Returns:
            Liste d'événements web
        """
        events = []
        
        # Parcourir chaque client CRM et générer des événements web associés
        for client in self.crm_data["clients"]:
            # Nombre d'événements à générer pour ce client (basé sur son segment)
            segment_weights = {
                "vip": (5, 15),
                "régulier": (3, 10),
                "occasionnel": (1, 5),
                "nouveau": (1, 8),
                "inactif": (0, 2)
            }
            
            # Déterminer le nombre d'événements pour ce client
            min_events, max_events = segment_weights.get(client["segment"], (1, 3))
            client_events_count = random.randint(min_events, max_events)
            
            if client_events_count > 0:
                # Créer une session pour ce client
                session_id = self.web_generator.generate_session()
                self.session_user_mapping[session_id] = client["client_id"]
                
                # Créer un utilisateur authentifié
                user = {
                    "user_id": f"U{client['client_id']}",
                    "email": client["email"],
                    "authenticated": True,
                    "registration_date": client["inscription"],
                    "user_segment": client["segment"],
                    "phone": client["telephone"]
                }
                
                try:
                    # Générer un parcours utilisateur complet avec gestion d'erreur
                    journey = self.web_generator.generate_user_journey(client_events_count)
                    
                    # Remplacer les informations utilisateur par celles du client CRM
                    for event in journey:
                        event["user"] = user
                        event["session_id"] = session_id
                        
                        # Lier éventuellement à une campagne marketing en vérifiant que le dictionnaire marketing existe
                        if random.random() < 0.3:  # 30% de chance d'être lié à une campagne
                            campaign_id = random.choice(self.campaign_ids)
                            campaign = self.campaign_dict[campaign_id]
                            
                            # S'assurer que le dictionnaire marketing existe
                            if "marketing" not in event:
                                event["marketing"] = {}
                            
                            # Modifier la source et le medium selon la campagne
                            platform = campaign["platform"]
                            if "Facebook" in platform:
                                event["marketing"]["source"] = "facebook"
                                event["marketing"]["medium"] = "social"
                            elif "Google" in platform:
                                event["marketing"]["source"] = "google"
                                event["marketing"]["medium"] = "cpc"
                            elif "TikTok" in platform:
                                event["marketing"]["source"] = "tiktok"
                                event["marketing"]["medium"] = "social"
                            
                            event["marketing"]["campaign"] = f"camp_{campaign_id}"
                            event["marketing"]["campaign_id"] = campaign_id
                    
                    events.extend(journey)
                except Exception as e:
                    logger.error(f"Erreur lors de la génération du parcours utilisateur: {str(e)}")
                    continue
                
                if len(events) >= count:
                    break
        
        # Générer des événements pour utilisateurs anonymes si nécessaire pour atteindre le count
        attempts = 0
        max_attempts = 100  # Limite pour éviter une boucle infinie
        
        while len(events) < count and attempts < max_attempts:
            try:
                events.append(self.web_generator.generate_event())
                attempts = 0  # Réinitialiser le compteur si ça fonctionne
            except Exception as e:
                logger.warning(f"Échec de génération d'événement: {str(e)}")
                attempts += 1
        
        # Tronquer à la taille demandée
        return events[:count]
    
    def generate_aligned_datasets(self, web_events_count=1000):
        """
        Génère un ensemble complet de données alignées entre les trois sources
        
        Returns:
            Dictionnaire contenant les trois ensembles de données
        """
        web_events = self.generate_aligned_web_events(web_events_count)
        
        logger.info(f"Génération de {len(web_events)} événements web alignés")
        
        # Calculer des statistiques d'alignement
        authenticated_events = sum(1 for e in web_events if e["user"].get("authenticated", False))
        campaign_linked_events = sum(1 for e in web_events if "campaign_id" in e.get("marketing", {}))
        
        logger.info(f"Événements utilisateurs authentifiés: {authenticated_events} ({authenticated_events/len(web_events)*100:.1f}%)")
        logger.info(f"Événements liés à des campagnes: {campaign_linked_events} ({campaign_linked_events/len(web_events)*100:.1f}%)")
        
        return {
            "web_events": web_events,
            "crm_data": self.crm_data,
            "advertising_data": self.ad_data
        }
    
    def save_datasets(self, output_dir="datasets", formatted=True):
        """
        Sauvegarde les ensembles de données alignés dans des fichiers JSON
        
        Args:
            output_dir: Répertoire de sortie
            formatted: Si True, les JSON seront formatés avec indentation
        """
        os.makedirs(output_dir, exist_ok=True)
        
        # Générer les données alignées
        datasets = self.generate_aligned_datasets()
        
        # Sauvegarder chaque ensemble de données
        indent = 2 if formatted else None
        
        with open(os.path.join(output_dir, "web_events.json"), "w", encoding="utf-8") as f:
            json.dump(datasets["web_events"], f, ensure_ascii=False, indent=indent)
            
        with open(os.path.join(output_dir, "crm_data.json"), "w", encoding="utf-8") as f:
            json.dump(datasets["crm_data"], f, ensure_ascii=False, indent=indent)
            
        with open(os.path.join(output_dir, "advertising_data.json"), "w", encoding="utf-8") as f:
            json.dump(datasets["advertising_data"], f, ensure_ascii=False, indent=indent)
        
        logger.info(f"Ensembles de données sauvegardés dans le répertoire '{output_dir}'")
    
    def generate_mapping_report(self):
        """
        Génère un rapport sur les liens entre les différentes sources de données
        
        Returns:
            Dictionnaire avec les statistiques d'alignement
        """
        # Compter les sessions par client
        sessions_per_client = {}
        for session_id, client_id in self.session_user_mapping.items():
            if client_id not in sessions_per_client:
                sessions_per_client[client_id] = []
            sessions_per_client[client_id].append(session_id)
        
        # Analyser les campagnes et leur impact
        campaign_stats = {}
        for campaign_id in self.campaign_ids:
            campaign = self.campaign_dict[campaign_id]
            campaign_stats[campaign_id] = {
                "name": campaign["name"],
                "platform": campaign["platform"],
                "start_date": campaign["start_date"],
                "end_date": campaign["end_date"],
                "budget": campaign["total_budget"],
                "impacted_clients": 0,
                "web_events": 0
            }
        
        # Construire le rapport
        report = {
            "timestamp": datetime.now().isoformat(),
            "clients_total": len(self.client_ids),
            "campaigns_total": len(self.campaign_ids),
            "sessions_total": len(self.session_user_mapping),
            "clients_with_sessions": len(sessions_per_client),
            "avg_sessions_per_client": round(len(self.session_user_mapping) / max(1, len(sessions_per_client)), 2),
            "campaign_stats": campaign_stats
        }
        
        return report

if __name__ == "__main__":
    # Exemple d'utilisation
    aligner = DataAligner(seed=42)  # Utiliser seed pour reproductibilité
    
    # Sauvegarder les données alignées
    aligner.save_datasets(output_dir="aligned_datasets")
    
    # Générer un rapport d'alignement
    report = aligner.generate_mapping_report()
    print(json.dumps(report, indent=2))
