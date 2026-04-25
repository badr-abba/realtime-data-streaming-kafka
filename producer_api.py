import os
import time
import json
import schedule
import requests
from dotenv import load_dotenv
from confluent_kafka import Producer

# 1. Charger les variables d'environnement
load_dotenv()
API_KEY = os.getenv('OPENWEATHER_API_KEY')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
CITIES = ["Paris", "Casablanca", "Rabat", "Tanger"]
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
TOPIC_NAME = "topic_api_meteo"

# 2. Configuration du Producer Kafka
producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'python-weather-producer'
}
producer = Producer(producer_config)

# Fonction de callback (Callback = "Rappelle-moi quand c'est fini")
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Échec de la livraison : {err}")
    else:
        print(f"✅ Livré sur {msg.topic()} [Partition: {msg.partition()}] à l'offset {msg.offset()}")

# 3. Fonction de collecte et d'envoi
def fetch_weather():
    print(f"\n--- [API] Lancement de la collecte : {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    for city in CITIES:
        try:
            params = {
                'q': city,
                'appid': API_KEY,
                'units': 'metric'
            }
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Formatage de la donnée métier (Payload)
            weather_payload = {
                "city": data["name"],
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "weather": data["weather"][0]["description"],
                "timestamp": int(time.time())
            }
            
            # Transformation en JSON binaire et envoi à Kafka
            json_data = json.dumps(weather_payload).encode('utf-8')
            byte_key = city.encode('utf-8')
            
            producer.produce(
                topic=TOPIC_NAME,
                key=byte_key,
                value=json_data,
                callback=delivery_report
            )
            
            # Demande à librdkafka de dépiler ses envois en arrière-plan
            producer.poll(0)
            
        except requests.exceptions.RequestException as e:
            print(f"Erreur lors de l'appel API pour {city}: {e}")
        except Exception as e:
            print(f"Erreur inattendue pour {city}: {e}")

if __name__ == "__main__":
    print(f"Démarrage du Producer API Météo vers {KAFKA_BROKER} (Timer: 60s)...")
    
    fetch_weather()
    schedule.every(60).seconds.do(fetch_weather)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nArrêt manuel du script (Ctrl+C). Purge des messages en attente...")
        producer.flush() # Attend que les derniers messages partent avant de s'éteindre
        print("Arrêt complet.")
