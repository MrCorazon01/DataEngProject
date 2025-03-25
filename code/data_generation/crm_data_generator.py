import random
import json
import uuid
import time
from datetime import datetime, timedelta
from faker import Faker

fake = Faker('fr_FR')

class CRMDataGenerator:
    def __init__(self):
        """Initialisation du générateur de données CRM"""
        # Types d'interactions clients
        self.interaction_types = [
            "appel_service_client", "email_support", "réclamation", "demande_information", 
            "achat_en_boutique", "retour_produit", "inscription_newsletter", "avis_produit",
            "demande_échantillon", "participation_événement"
        ]
        
        # Sources d'acquisition clients
        self.acquisition_sources = [
            "site_web", "recommandation", "publicité_facebook", "publicité_google",
            "marketplace", "boutique_physique", "foire_expo", "partenariat",
            "influenceur", "programme_fidélité"
        ]
        
        # Segments clients
        # Correction: s'assurer que les fréquences d'achat min (premier élément du tuple) ne sont jamais 0
        self.segments = {
            "vip": {"freq_achat": (20, 60), "panier_moyen": (15000, 50000)},
            "régulier": {"freq_achat": (60, 120), "panier_moyen": (8000, 20000)},
            "occasionnel": {"freq_achat": (120, 365), "panier_moyen": (5000, 12000)},
            "nouveau": {"freq_achat": (1, 30), "panier_moyen": (3000, 10000)},  # Changé de 0 à 1
            "inactif": {"freq_achat": (365, 730), "panier_moyen": (2000, 8000)}
        }
        
        # Préférences de communication
        self.communication_prefs = ["email", "sms", "téléphone", "courrier", "toutes"]
        
        # Statut fidélité
        self.fidelity_status = ["bronze", "argent", "or", "platine", "non_inscrit"]
        
        # Villes pour regroupement géographique
        self.cities = [
            "Dakar", "Thiès", "Rufisque", "Saint-Louis", "Touba", "Kaolack",
            "Ziguinchor", "Mbour", "Diourbel", "Louga", "Tambacounda", "Kolda"
        ]

    def generate_senegal_phone(self):
        """Génère un numéro de téléphone sénégalais"""
        prefix = random.choice(['77', '78', '76', '70', '75', '76', '70'])
        suffix = ''.join([str(random.randint(0, 9)) for _ in range(7)])
        return f"+221{prefix}{suffix}"
    
    def generate_client(self):
        """Génère un client CRM complet"""
        # Base de données personnelles
        is_male = random.choice([True, False])
        
        # Segment du client
        segment = random.choices(
            list(self.segments.keys()),
            weights=[0.05, 0.25, 0.4, 0.2, 0.1],
            k=1
        )[0]
        
        # Date d'inscription (entre 3 ans et aujourd'hui)
        max_days_ago = 365 * 3
        registration_date = datetime.now() - timedelta(days=random.randint(0, max_days_ago))
        
        # Date dernier achat basé sur segment
        min_days, max_days = self.segments[segment]["freq_achat"]
        last_purchase = datetime.now() - timedelta(days=random.randint(min_days, max_days))
        
        # Valeur client (LTV) basée sur segment et temps écoulé depuis inscription
        days_since_reg = (datetime.now() - registration_date).days
        
        # Correction: Protection contre la division par zéro
        freq_achat_min = max(1, self.segments[segment]["freq_achat"][0])  # S'assurer que min est au moins 1
        purchases_count = max(1, int(days_since_reg / freq_achat_min * 0.8))
        
        avg_basket = random.uniform(*self.segments[segment]["panier_moyen"])
        ltv = int(purchases_count * avg_basket)
        
        client_id = random.randint(10000, 99999)
        
        return {
            "client_id": client_id,
            "nom": fake.last_name(),
            "prenom": fake.first_name_male() if is_male else fake.first_name_female(),
            "email": fake.email(),
            "telephone": self.generate_senegal_phone(),
            "date_naissance": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
            "adresse": {
                "rue": fake.street_address(),
                "ville": random.choice(self.cities),
                "code_postal": f"{random.randint(10000, 99999)}",
                "pays": "Sénégal"
            },
            "inscription": registration_date.isoformat(),
            "segment": segment,
            "source_acquisition": random.choice(self.acquisition_sources),
            "preferences_communication": random.choice(self.communication_prefs),
            "statut_fidelite": random.choice(self.fidelity_status) if random.random() > 0.3 else "non_inscrit",
            "dernier_achat": last_purchase.isoformat(),
            "valeur_client": ltv,
            "nombre_achats": purchases_count,
            "panier_moyen": int(avg_basket),
            "consentements": {
                "marketing": random.choice([True, False]),
                "newsletter": random.choice([True, False]),
                "analyse_donnees": random.choice([True, False])
            }
        }
    
    def generate_interaction(self, client_id, interaction_date=None):
        """Génère une interaction client"""
        if not interaction_date:
            # Générer une date entre 1 et 90 jours dans le passé
            days_ago = random.randint(1, 90)
            interaction_date = datetime.now() - timedelta(days=days_ago)
        
        interaction_type = random.choice(self.interaction_types)
        
        # Contenu de l'interaction selon le type
        content = ""
        if "appel" in interaction_type:
            content = fake.paragraph(nb_sentences=2) + "\nDurée: " + str(random.randint(1, 30)) + " minutes"
        elif "email" in interaction_type:
            content = "Objet: " + fake.sentence() + "\n" + fake.paragraph(nb_sentences=3)
        elif "réclamation" in interaction_type:
            content = "Réclamation concernant: " + random.choice(["livraison", "produit", "service", "remboursement"]) + "\n" + fake.paragraph(nb_sentences=2)
        elif "achat" in interaction_type:
            content = "Achat de " + str(random.randint(1, 5)) + " produits pour un total de " + str(random.randint(5000, 50000)) + " XOF"
        else:
            content = fake.paragraph(nb_sentences=1)
        
        # Statut de l'interaction
        if interaction_type == "réclamation":
            status = random.choice(["en_cours", "résolu", "en_attente", "escaladé"])
        elif "achat" in interaction_type:
            status = "complété"
        else:
            status = random.choice(["complété", "en_cours", "planifié", "annulé"])
        
        return {
            "interaction_id": str(uuid.uuid4()),
            "client_id": client_id,
            "date": interaction_date.isoformat(),
            "type": interaction_type,
            "canal": random.choice(["téléphone", "email", "boutique", "site_web", "app_mobile", "social_media"]),
            "contenu": content,
            "statut": status,
            "agent": f"agent_{random.randint(1, 20)}",
            "satisfaction": random.randint(1, 5) if random.random() > 0.3 else None,
            "tags": random.sample(["urgent", "vip", "fidélité", "information", "technique", "commercial", "logistique", "produit"], k=random.randint(0, 3))
        }
    
    def generate_crm_data(self, num_clients=100, min_interactions=1, max_interactions=10):
        """Génère un ensemble complet de données CRM"""
        clients = []
        interactions = []
        
        for _ in range(num_clients):
            client = self.generate_client()
            clients.append(client)
            
            # Générer plusieurs interactions pour ce client
            num_interactions = random.randint(min_interactions, max_interactions)
            for _ in range(num_interactions):
                interaction = self.generate_interaction(client["client_id"])
                interactions.append(interaction)
        
        return {
            "clients": clients,
            "interactions": interactions
        }
    
    def get_client_ids(self):
        """Retourne les IDs clients pour l'alignement avec d'autres sources"""
        if hasattr(self, 'clients'):
            return [client["client_id"] for client in self.clients]
        return []

if __name__ == "__main__":
    generator = CRMDataGenerator()
    crm_data = generator.generate_crm_data(num_clients=10, min_interactions=2, max_interactions=5)
    print(json.dumps(crm_data, indent=2))
