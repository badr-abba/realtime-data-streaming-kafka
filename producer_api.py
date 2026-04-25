import os
import time
import json
import schedule
import requests
from dotenv import load_dotenv
from confluent_kafka import Producer

# Charger les variables d'environnement
load_dotenv()
API_KEY = os.getenv('OPENWEATHER_API_KEY')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
CITIES = ["Paris", "Casablanca", "Rabat", "Tanger"]
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"
TOPIC_NAME = "topic_api_meteo"

# Configurer le Producer Kafka
producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'python-weather-producer'
}
producer = Producer(producer_config)

# Fonction appelee lors de l'envoi d'un message (succes ou echec)
def delivery_report(err, msg):
    if err is not None:
        print(f"Echec de l'envoi : {err}")
    else:
        print(f"Message envoye sur {msg.topic()} [Partition: {msg.partition()}] a l'offset {msg.offset()}")

# Fonction pour recuperer la meteo et l'envoyer a Kafka
def fetch_weather():
    print(f"\n--- Collecte demarree : {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
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
            
            # Preparer le dictionnaire de donnees
            weather_payload = {
                "city": data["name"],
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "weather": data["weather"][0]["description"],
                "timestamp": int(time.time())
            }
            
            # Envoyer le message en format JSON binaire a Kafka
            producer.produce(
                topic=TOPIC_NAME,
                key=city.encode('utf-8'),
                value=json.dumps(weather_payload).encode('utf-8'),
                callback=delivery_report
            )
            
            # Permet au producer de traiter les envois en arriere-plan
            producer.poll(0)
            
        except requests.exceptions.RequestException as e:
            print(f"Erreur d'appel API pour {city}: {e}")
        except Exception as e:
            print(f"Erreur inattendue pour {city}: {e}")

if __name__ == "__main__":
    print(f"Demarrage du Producer vers {KAFKA_BROKER} (Toutes les 60s)...")
    
    # Premier appel immediat
    fetch_weather()
    
    # Planifier l'execution toutes les 60 secondes
    schedule.every(60).seconds.do(fetch_weather)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1) # Pause pour eviter la surcharge du CPU
    except KeyboardInterrupt:
        print("\nArret manuel demande. Purge des messages restants...")
        producer.flush() # Envoyer les messages bloques en memoire avant l'arret
        print("Fin du programme.")
