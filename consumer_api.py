import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

# Charger les variables d'environnement
load_dotenv()
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = "topic_api_meteo"

# Configurer le Consumer Kafka
consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'meteo-processor-group',
    'auto.offset.reset': 'earliest', # Lire l'historique au premier lancement
    'enable.auto.commit': False      # Desactiver la validation automatique
}
consumer = Consumer(consumer_config)
consumer.subscribe([TOPIC_NAME])

# Registre pour ignorer les doublons (idempotence)
processed_messages = set()

# Memoire temporaire pour traiter les messages par lots (Batch)
message_batch = []
BATCH_SIZE = 10

print(f"Demarrage du Consumer sur {TOPIC_NAME} (Lot de {BATCH_SIZE} messages)...")
print("En attente de messages (Ctrl+C pour arreter)\n")

try:
    while True:
        # Lire les messages avec 1 seconde d'attente maximum
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Erreur Kafka: {msg.error()}")
                break
            continue
                
        # Decoder le message JSON
        try:
            val_str = msg.value().decode('utf-8')
            data = json.loads(val_str)
            
            # Creer un identifiant unique : Ville + Timestamp
            msg_id = f"{data['city']}_{data['timestamp']}"
            
            # Verifier si le message est un doublon
            if msg_id in processed_messages:
                print(f"[Ignore] Message en double : {msg_id}")
                continue
                
            print(f"[Lu] {data['city']} : {data['temperature']}C")
            
            # Ajouter des informations utiles avant sauvegarde
            data['_metadata'] = {
                'source': 'api_openweather',
                'processed_at': datetime.now().isoformat()
            }
            
            # Stocker en memoire
            message_batch.append(data)
            processed_messages.add(msg_id)
            
            # Sauvegarder le lot sur disque s'il est plein
            if len(message_batch) >= BATCH_SIZE:
                os.makedirs('data_lake/api_meteo', exist_ok=True)
                
                # Definir le nom du fichier
                date_str = datetime.now().strftime('%Y-%m-%d-%H%M')
                filename = f"data_lake/api_meteo/meteo_export_{date_str}.jsonl"
                
                # Ecrire le lot complet dans le fichier
                with open(filename, 'a', encoding='utf-8') as f:
                    for item in message_batch:
                        f.write(json.dumps(item, ensure_ascii=False) + '\n')
                
                print(f"[Batch] {BATCH_SIZE} messages sauvegardes dans {filename}")
                
                # Valider manuellement l'avancement dans Kafka
                consumer.commit(asynchronous=False)
                print("[Commit] Offset valide.\n")
                
                # Vider la memoire pour le prochain lot
                message_batch.clear()
                
        except json.JSONDecodeError:
            print("Erreur de decodage JSON.")

except KeyboardInterrupt:
    print("\nArret manuel en cours...")
    if message_batch:
        print(f"Attention : {len(message_batch)} messages non sauvegardes seront retraites plus tard.")
finally:
    # Toujours fermer le consumer pour liberer les ressources du serveur
    consumer.close()
    print("Consumer arrete correctement.")
