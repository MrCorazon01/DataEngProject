import random
import json
import uuid
import time
from datetime import datetime, timedelta
from faker import Faker

fake = Faker('fr_FR')

class WebEventGenerator:
    def __init__(self, crm_ids=None, campaign_ids=None):
        # Permettre l'injection d'IDs CRM et campagnes pour l'alignement
        self.crm_ids = crm_ids or []  # Liste d'IDs clients pour alignement avec CRM
        self.campaign_ids = campaign_ids or []  # Liste d'IDs campagnes pour alignement avec données publicitaires
        
        # Définition des pages et produits du site e-commerce
        self.pages = [
            "/", "/produits", "/categories", "/panier", "/mon-compte",
            "/checkout", "/confirmation", "/support", "/a-propos", "/blog"
        ]
        
        self.categories = ["visage", "corps", "cheveux", "maquillage", "homme", "bio", "naturel", "traditionnel", "karité", "baobab"]
        
        self.produits = [
            {"id": "P1001", "nom": "Crème hydratante au karité", "prix": 12500, "categorie": "visage"},
            {"id": "P1002", "nom": "Sérum à l'huile de baobab", "prix": 18000, "categorie": "visage"},
            {"id": "P1003", "nom": "Huile de coco pressée à froid", "prix": 9500, "categorie": "corps"},
            {"id": "P1004", "nom": "Savon noir traditionnel", "prix": 4000, "categorie": "corps"},
            {"id": "P1005", "nom": "Masque purifiant à l'argile du Sahel", "prix": 11000, "categorie": "visage"},
            {"id": "P1006", "nom": "Beurre de karité pur", "prix": 7500, "categorie": "corps"},
            {"id": "P1007", "nom": "Huile d'argan bio", "prix": 15000, "categorie": "cheveux"},
            {"id": "P1008", "nom": "Crème solaire naturelle SPF30", "prix": 13500, "categorie": "corps"},
            {"id": "P1009", "nom": "Baume à lèvres au miel", "prix": 3500, "categorie": "visage"},
            {"id": "P1010", "nom": "Eau florale de rose du Maghreb", "prix": 9000, "categorie": "visage"},
            {"id": "P1011", "nom": "Henné naturel du Sénégal", "prix": 5000, "categorie": "cheveux"},
            {"id": "P1012", "nom": "Lotion après-rasage au moringa", "prix": 8000, "categorie": "homme"},
            {"id": "P1013", "nom": "Shampooing aux extraits de kola", "prix": 6500, "categorie": "cheveux"},
            {"id": "P1014", "nom": "Gommage au sel de mer de Dakar", "prix": 10000, "categorie": "corps"},
            {"id": "P1015", "nom": "Dentifrice naturel au siwak", "prix": 4500, "categorie": "traditionnel"}
        ]
        
        # Types d'événements
        self.event_types = ["page_view", "product_view", "add_to_cart", "remove_from_cart", 
                           "begin_checkout", "purchase", "search", "filter_products", "login", "signup"]
        
        # Navigateurs et appareils
        self.browsers = ["Chrome Mobile", "Opera Mini", "Chrome", "Firefox", "Safari", "Edge", "Mobile Safari", "UC Browser"]
        self.devices = ["mobile", "mobile", "mobile", "tablet", "desktop"]
        
        self.traffic_sources = [
            "direct", "google", "facebook", "whatsapp", "instagram", "youtube",
            "jumia", "expat-dakar.com", "seneweb.com", "tiktok", "telegram", 
            "orange.sn", "refer-friend", "email", "sms"
        ]
        
        self.senegal_cities = [
            "Dakar", "Thiès", "Rufisque", "Saint-Louis", "Touba", "Kaolack",
            "Ziguinchor", "Mbour", "Diourbel", "Louga", "Tambacounda", "Kolda",
            "Mbacké", "Tivaouane", "Joal-Fadiouth", "Kaffrine", "Matam", "Vélingara"
        ]
        
        self.telecom_operators = ["Orange", "Free", "Expresso"]
        
        self.payment_methods = ["orange_money", "free_money", "wave", "card", "cash_on_delivery", "paypal"]
        
        self.coupons = [None, None, None, "TABASKI10", "BIENVENUE", "RAMADAN20", "NOEL15", "DECOUVERTE"]

        self.senegal_email_domains = ["orange.sn", "gmail.com", "yahoo.fr", "hotmail.fr", "outlook.com", "free.sn"]

        # Ajout de nouvelles propriétés pour la durée de visite
        self.page_avg_durations = {
            "/": (20, 60),  # (min, max) en secondes
            "/produits": (30, 180),
            "/categories": (20, 90),
            "/panier": (40, 120),
            "/mon-compte": (30, 150),
            "/checkout": (60, 300),
            "/confirmation": (10, 40),
            "/support": (40, 240),
            "/a-propos": (15, 60),
            "/blog": (60, 360)
        }
        
        # Sources de trafic utilisées dans les campagnes publicitaires
        self.ad_traffic_sources = {
            "facebook": ["fb_newsfeed", "fb_stories", "instagram", "fb_marketplace"],
            "google": ["search", "display", "youtube", "discovery"],
            "tiktok": ["feed", "topview", "hashtag_challenge"],
            "email": ["newsletter", "promo", "abandoned_cart", "welcome"]
        }

    def generate_session(self):
        """Génère une nouvelle session utilisateur"""
        return str(uuid.uuid4())
    
    def generate_senegal_email(self):
        """Génère un email avec un nom local et un domaine """
        name = fake.user_name().lower().replace(' ', '.')
        domain = random.choice(self.senegal_email_domains)
        return f"{name}@{domain}"
    
    def generate_senegal_phone(self):
        """Génère un numéro de téléphone"""
        prefix = random.choice(['77', '78', '76', '70', '75', '76', '70'])
        suffix = ''.join([str(random.randint(0, 9)) for _ in range(7)])
        return f"+221{prefix}{suffix}"
    
    def generate_user(self, authenticated=None):
        """Génère un utilisateur (anonyme ou authentifié)"""
        is_authenticated = authenticated if authenticated is not None else random.random() < 0.3
        
        if is_authenticated:
            # Si nous avons des IDs CRM disponibles, utilisons-en un pour l'alignement
            if self.crm_ids and random.random() < 0.8:  # 80% de chance d'utiliser un ID CRM existant
                crm_id = random.choice(self.crm_ids)
                user_id = f"U{crm_id}"  # Préfixe U pour indiquer que c'est un ID utilisateur web
            else:
                user_id = f"U{random.randint(10000, 99999)}"
                
            user = {
                "user_id": user_id,
                "email": self.generate_senegal_email(),
                "authenticated": True,
                "registration_date": fake.date_time_this_year().isoformat(),
                "user_segment": random.choice(["new", "regular", "vip", "diaspora"]),
                "phone": self.generate_senegal_phone()
            }
        else:
            user = {
                "user_id": None,
                "authenticated": False
            }
        
        return user
    
    def generate_event(self, session_id=None, user=None):
        """Génère un événement web """
        # Créer une nouvelle session si non fournie
        if not session_id:
            session_id = self.generate_session()
            
        # Créer un utilisateur si non fourni
        if not user:
            user = self.generate_user()
        
        # Type d'événement
        event_type = random.choice(self.event_types)
        
        # Base commune pour tous les événements
        event_base = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            "user": user,
            "device": {
                "type": random.choice(self.devices),
                "browser": random.choice(self.browsers),
                "os": random.choice(["Android", "Android", "iOS", "Windows", "MacOS", "Linux"]),  # Plus d'Android
                "resolution": random.choice(["360x640", "375x667", "414x896", "1366x768", "1280x720", "1920x1080"]),
                "operator": random.choice(self.telecom_operators) if random.random() < 0.7 else None
            },
            "location": {
                "country": "Sénégal",
                "city": random.choice(self.senegal_cities),
                "language": "fr",
                "region": random.choice(["Dakar", "Thiès", "Saint-Louis", "Diourbel", "Fatick", "Kaolack", "Kaffrine", 
                                       "Kédougou", "Kolda", "Louga", "Matam", "Sédhiou", "Tambacounda", "Ziguinchor"])
            },
            "marketing": {
                "source": random.choice(self.traffic_sources),
                "medium": random.choice(["cpc", "organic", "social", "email", "direct", "sms", "referral"]),
                "campaign": random.choice([None, "tabaski_2023", "ramadan_promo", "nouveaux_produits", "reactivation"])
            }
        }
        
        # Ajouter la référence à la campagne si applicable
        if self.campaign_ids and random.random() < 0.4:  # 40% de chance d'être lié à une campagne
            campaign_id = random.choice(self.campaign_ids)
            selected_source = None
            
            # Correction: Vérification sécurisée si campaign est None
            current_campaign = event_base["marketing"]["campaign"]
            campaign_str = f"camp_{campaign_id}"
            
            # Vérifier si la campagne actuelle contient déjà l'ID ou choisir au hasard
            if (current_campaign and campaign_str in current_campaign) or random.random() < 0.3:
                # Trouver une source compatible avec cette campagne
                for source, channels in self.ad_traffic_sources.items():
                    selected_source = source
                    event_base["marketing"]["source"] = source
                    event_base["marketing"]["campaign"] = campaign_str
                    event_base["marketing"]["channel"] = random.choice(channels)
                    event_base["marketing"]["campaign_id"] = campaign_id
                    break
        
        # Données spécifiques selon le type d'événement
        if event_type == "page_view":
            page = random.choice(self.pages)
            # Ajouter la durée de visite pour les vues de page
            min_duration, max_duration = self.page_avg_durations.get(page, (10, 120))
            duration = random.randint(min_duration, max_duration)
            
            event_base["page"] = {
                "url": f"https://www.biocosmetics.sn{page}",
                "referrer": random.choice([None, "https://www.google.sn", "https://www.facebook.com", 
                                         "https://www.instagram.com", "https://www.seneweb.com", 
                                         "https://www.expat-dakar.com", "https://www.biocosmetics.sn/"]),
                "title": f"BioCosmetics Sénégal - {page.replace('/', '').capitalize() or 'Accueil'}",
                "visit_duration": duration  # Ajout de la durée de visite en secondes
            }
            
        elif event_type == "product_view":
            produit = random.choice(self.produits)
            event_base["product"] = {
                "product_id": produit["id"],
                "name": produit["nom"],
                "price": produit["prix"],
                "category": produit["categorie"],
                "url": f"https://www.biocosmetics.sn/produits/{produit['id']}"
            }
            
        elif event_type in ["add_to_cart", "remove_from_cart"]:
            produit = random.choice(self.produits)
            event_base["product"] = {
                "product_id": produit["id"],
                "name": produit["nom"],
                "price": produit["prix"],
                "category": produit["categorie"],
                "quantity": random.randint(1, 3)
            }
            
        elif event_type == "search":
            event_base["search"] = {
                "term": random.choice(["karité", "baobab", "naturel", "bio", "savon noir", "huile", "cheveux", "visage", "argan", "traditionnel"]),
                "results_count": random.randint(0, 25)
            }
            
        elif event_type == "filter_products":
            event_base["filter"] = {
                "category": random.choice(self.categories),
                "price_range": random.choice(["0-5000", "5000-10000", "10000-20000", "20000+"]),
                "sort_by": random.choice(["popularity", "price_asc", "price_desc", "newest"])
            }
            
        elif event_type == "begin_checkout":
            # Générer entre 1 et 4 produits dans le panier
            cart_products = []
            cart_value = 0
            for _ in range(random.randint(1, 4)):
                produit = random.choice(self.produits)
                quantity = random.randint(1, 2)
                cart_products.append({
                    "product_id": produit["id"],
                    "name": produit["nom"],
                    "price": produit["prix"],
                    "quantity": quantity
                })
                cart_value += produit["prix"] * quantity
                
            event_base["checkout"] = {
                "cart_id": f"cart_{uuid.uuid4().hex[:8]}",
                "items": cart_products,
                "value": cart_value,
                "coupon": random.choice(self.coupons),
                "step": 1,
                "shipping_zone": random.choice(["Dakar Centre", "Dakar Périphérie", "Grand Dakar", "Autres régions"])
            }
            
        elif event_type == "purchase":
            # Générer entre 1 et 4 produits dans la commande
            order_products = []
            order_value = 0
            for _ in range(random.randint(1, 4)):
                produit = random.choice(self.produits)
                quantity = random.randint(1, 2)
                order_products.append({
                    "product_id": produit["id"],
                    "name": produit["nom"],
                    "price": produit["prix"],
                    "quantity": quantity
                })
                order_value += produit["prix"] * quantity
                
            # Calculer les frais de livraison basés sur la zone
            shipping_zone = random.choice(["Dakar Centre", "Dakar Périphérie", "Grand Dakar", "Autres régions"])
            shipping = 0 if order_value > 20000 else (
                1000 if shipping_zone == "Dakar Centre" else 
                2000 if shipping_zone == "Dakar Périphérie" else
                3000 if shipping_zone == "Grand Dakar" else 5000
            )
            
            order_total = order_value + shipping
            
            event_base["purchase"] = {
                "order_id": f"ORD-{int(time.time())}-{random.randint(100, 999)}",
                "items": order_products,
                "subtotal": order_value,
                "shipping": shipping,
                "shipping_zone": shipping_zone,
                "tax": round(order_total * 0.18),  # TVA 18% au Sénégal
                "total": order_total,
                "payment_method": random.choice(self.payment_methods),
                "shipping_method": random.choice(["standard", "express", "point_relais", "boutique"]),
                "currency": "XOF"  # Franc CFA, monnaie du Sénégal
            }
        
        return event_base

    def generate_user_journey(self, events_count=5):
        """Génère une séquence d'événements pour simuler un parcours utilisateur"""
        session_id = self.generate_session()
        is_authenticated = random.random() < 0.3
        user = self.generate_user(authenticated=is_authenticated)
        
        # Premier événement est toujours une vue de page
        journey = [self.generate_event(session_id, user)]
        journey[0]["event_type"] = "page_view"
        
        for _ in range(events_count - 1):
            new_event = self.generate_event(session_id, user)
            # Logique pour rendre la séquence plus réaliste
            if journey[-1]["event_type"] == "purchase":
                # Après un achat, on regarde souvent une autre page
                new_event["event_type"] = "page_view"
            elif journey[-1]["event_type"] == "add_to_cart":
                # Après ajout au panier, possibilité de checkout ou continuer shopping
                new_event["event_type"] = random.choice(
                    ["page_view", "product_view", "begin_checkout", "add_to_cart"]
                )
            
            journey.append(new_event)
                
        return journey

if __name__ == "__main__":
    generator = WebEventGenerator()
    # Test: génération d'un événement
    print(json.dumps(generator.generate_event(), indent=2))
    # Test: génération d'un parcours utilisateur
    journey = generator.generate_user_journey(4)
    print(f"Journey with {len(journey)} events:")
    for evt in journey:
        print(f"- {evt['event_type']}")
