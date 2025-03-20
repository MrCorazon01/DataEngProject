import random
import json
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker('fr_FR')

class AdvertisingDataGenerator:
    def __init__(self):
        """Initialisation du générateur de données publicitaires"""
        # Plateformes publicitaires
        self.ad_platforms = ["Facebook Ads", "Google Ads", "TikTok Ads", "Instagram Ads", "Snapchat Ads", "YouTube Ads"]
        
        # Types de campagnes
        self.campaign_types = ["awareness", "consideration", "conversion", "retargeting", "loyalty", "seasonal"]
        
        # Objectifs des campagnes
        self.campaign_objectives = {
            "awareness": ["brand_awareness", "reach"],
            "consideration": ["traffic", "engagement", "video_views", "lead_generation"],
            "conversion": ["conversions", "catalog_sales", "store_visits"],
            "retargeting": ["conversions", "catalog_sales"],
            "loyalty": ["engagement", "conversions"],
            "seasonal": ["reach", "conversions", "catalog_sales"]
        }
        
        # Formats publicitaires
        self.ad_formats = ["carousel", "single_image", "video", "stories", "collection", "messenger"]
        
        # Ciblages géographiques
        self.geo_targets = ["dakar", "senegal", "west_africa", "francophone_africa", "diaspora"]
        
        # Ciblages démographiques
        self.demographics = [
            {"age_min": 18, "age_max": 24, "gender": "all"},
            {"age_min": 18, "age_max": 24, "gender": "female"},
            {"age_min": 25, "age_max": 34, "gender": "female"},
            {"age_min": 25, "age_max": 34, "gender": "all"},
            {"age_min": 35, "age_max": 44, "gender": "female"},
            {"age_min": 35, "age_max": 54, "gender": "all"}
        ]
        
        # Centres d'intérêt pour ciblage
        self.interests = [
            "beauty", "cosmetics", "natural_products", "organic", "skincare", 
            "hair_care", "wellness", "fashion", "sustainability", "luxury"
        ]
        
        # Produits mis en avant
        self.highlighted_products = [
            "crème karité", "huile baobab", "savon noir", "masque visage", 
            "beurre corporel", "shampoing naturel", "coffret cadeau", "gommage"
        ]
        
        # Périodes spéciales
        self.special_periods = [
            {"name": "Tabaski", "month": 6, "budget_factor": 1.5},
            {"name": "Ramadan", "month": 4, "budget_factor": 1.3},
            {"name": "Fin d'année", "month": 12, "budget_factor": 1.6},
            {"name": "Saint-Valentin", "month": 2, "budget_factor": 1.2},
            {"name": "Journée Femme", "month": 3, "budget_factor": 1.4}
        ]

    def generate_campaign(self, start_date=None):
        """Génère une campagne publicitaire"""
        campaign_id = random.randint(1000, 9999)
        
        # Déterminer type de campagne et plateforme
        campaign_type = random.choice(self.campaign_types)
        platform = random.choice(self.ad_platforms)
        objective = random.choice(self.campaign_objectives[campaign_type])
        
        # Déterminer période et dates
        if not start_date:
            # Générer une date dans les 12 derniers mois
            days_ago = random.randint(1, 365)
            start_date = datetime.now() - timedelta(days=days_ago)
        
        # Durée de la campagne (entre 7 et 60 jours)
        duration_days = random.randint(7, 60)
        end_date = start_date + timedelta(days=duration_days)
        
        # Vérifier si c'est une période spéciale
        budget_boost = 1.0
        campaign_name = f"Camp{campaign_id} - "
        
        for period in self.special_periods:
            if start_date.month == period["month"]:
                budget_boost = period["budget_factor"]
                campaign_name += f"{period['name']} - "
        
        # Compléter le nom de la campagne
        campaign_name += f"{campaign_type.capitalize()} - {platform}"
        
        # Budget quotidien et total
        daily_budget = random.randint(5000, 50000) * budget_boost
        total_budget = daily_budget * duration_days
        
        # Ciblage
        targeting = {
            "geo": random.choice(self.geo_targets),
            "demographics": random.choice(self.demographics),
            "interests": random.sample(self.interests, k=random.randint(1, 4)),
            "custom_audiences": random.choice([None, "website_visitors", "past_customers", "cart_abandoners"]) if random.random() > 0.5 else None
        }
        
        # Produits mis en avant
        promoted_products = random.sample(self.highlighted_products, k=random.randint(1, 3))
        
        return {
            "campaign_id": campaign_id,
            "name": campaign_name,
            "platform": platform,
            "type": campaign_type,
            "objective": objective,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "status": "active" if end_date > datetime.now() else "completed",
            "daily_budget": int(daily_budget),
            "total_budget": int(total_budget),
            "targeting": targeting,
            "ad_format": random.choice(self.ad_formats),
            "promoted_products": promoted_products,
            "utm_parameters": {
                "source": platform.lower().split()[0],
                "medium": "cpc",
                "campaign": f"camp_{campaign_id}"
            }
        }
    
    def generate_daily_performance(self, campaign, date):
        """Génère des métriques de performance journalières pour une campagne"""
        # Vérifier si la date est comprise dans la période de la campagne
        campaign_start = datetime.fromisoformat(campaign["start_date"])
        campaign_end = datetime.fromisoformat(campaign["end_date"])
        
        if date < campaign_start or date > campaign_end:
            return None
        
        # Calculer le jour de la campagne (pour la progression)
        campaign_day = (date - campaign_start).days + 1
        campaign_duration = (campaign_end - campaign_start).days
        campaign_progress = campaign_day / campaign_duration
        
        # Budget quotidien
        daily_budget = campaign["daily_budget"]
        
        # Dépenses (90-100% du budget quotidien)
        spend = daily_budget * random.uniform(0.9, 1.0)
        
        # Impressions (variable selon le type de campagne)
        cpm = 0  # Coût pour mille impressions
        if campaign["objective"] in ["brand_awareness", "reach"]:
            cpm = random.uniform(500, 1000)
        elif campaign["objective"] in ["traffic", "engagement", "video_views"]:
            cpm = random.uniform(1000, 2000)
        else:
            cpm = random.uniform(1500, 3000)
            
        impressions = int((spend / cpm) * 1000)
        
        # CTR selon le type de campagne et le format
        base_ctr = 0
        if campaign["ad_format"] == "video":
            base_ctr = random.uniform(0.015, 0.03)
        elif campaign["ad_format"] == "carousel":
            base_ctr = random.uniform(0.01, 0.025)
        else:
            base_ctr = random.uniform(0.005, 0.02)
            
        # Ajustement CTR selon l'objectif
        if campaign["objective"] in ["conversions", "catalog_sales"]:
            base_ctr *= 1.2
        
        # CTR qui s'améliore légèrement avec l'optimisation
        ctr = base_ctr * (1 + campaign_progress * 0.2)
        
        # Clics
        clicks = int(impressions * ctr)
        
        # Conversions
        conversion_rate = 0
        if campaign["objective"] in ["conversions", "catalog_sales", "store_visits"]:
            conversion_rate = random.uniform(0.02, 0.05)
        elif campaign["objective"] == "lead_generation":
            conversion_rate = random.uniform(0.05, 0.1)
        else:
            conversion_rate = random.uniform(0.01, 0.02)
            
        conversions = int(clicks * conversion_rate)
        
        # Valeur des conversions (pour les campagnes de vente)
        conversion_value = 0
        if campaign["objective"] in ["conversions", "catalog_sales"]:
            avg_order_value = random.uniform(8000, 20000)
            conversion_value = conversions * avg_order_value
        
        return {
            "campaign_id": campaign["campaign_id"],
            "date": date.isoformat(),
            "platform": campaign["platform"],
            "spend": int(spend),
            "impressions": impressions,
            "clicks": clicks,
            "ctr": round(ctr * 100, 2),
            "conversions": conversions,
            "conversion_rate": round(conversion_rate * 100, 2) if conversions > 0 else 0,
            "cost_per_click": int(spend / clicks) if clicks > 0 else 0,
            "cost_per_conversion": int(spend / conversions) if conversions > 0 else 0,
            "conversion_value": int(conversion_value),
            "roas": round(conversion_value / spend, 2) if spend > 0 and conversion_value > 0 else 0
        }
    
    def generate_advertising_data(self, num_campaigns=10):
        """Génère un ensemble complet de données publicitaires"""
        campaigns = []
        daily_performances = []
        
        # Générer les campagnes
        for _ in range(num_campaigns):
            campaign = self.generate_campaign()
            campaigns.append(campaign)
            
            # Générer les performances quotidiennes pour chaque campagne
            start_date = datetime.fromisoformat(campaign["start_date"])
            end_date = datetime.fromisoformat(campaign["end_date"])
            
            current_date = start_date
            while current_date <= end_date and current_date <= datetime.now():
                daily_perf = self.generate_daily_performance(campaign, current_date)
                if daily_perf:
                    daily_performances.append(daily_perf)
                current_date += timedelta(days=1)
        
        return {
            "campaigns": campaigns,
            "daily_performances": daily_performances
        }
    
    def get_campaign_ids(self):
        """Retourne les IDs des campagnes pour l'alignement avec d'autres sources"""
        if hasattr(self, 'campaigns'):
            return [campaign["campaign_id"] for campaign in self.campaigns]
        return []

if __name__ == "__main__":
    generator = AdvertisingDataGenerator()
    ad_data = generator.generate_advertising_data(num_campaigns=5)
    print(json.dumps(ad_data, indent=2))
