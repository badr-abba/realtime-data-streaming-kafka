import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

# 1. Charger les variables d'environnement
load_dotenv()
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = "topic_api_meteo"

# 2. Configuration du Consumer Kafka
consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'meteo-processor-group',
    'auto.offset.reset': 'earliest', # Lit depuis le début si aucun offset n'est enregistré
    'enable.auto.commit': False      # On désactive l'auto-commit pour gérer la validation manuellement
}

consumer = Consumer(consumer_config)
consumer.subscribe([TOPIC_NAME])

# Registre en mémoire pour l'idempotence (Mémoire des messages déjà vus)
processed_messages = set()

# Mémoire tampon pour le traitement par lot (Batch Processing)
message_batch = []
BATCH_SIZE = 10

print(f"Démarrage du Consumer API Météo sur {TOPIC_NAME} (Mode Batch: {BATCH_SIZE})...")
print("En attente de nouveaux messages (Ctrl+C pour quitter)\n")

try:
    while True:
        # 3. Lecture bloquante pour 1 seconde (Polling)
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Fin de la partition atteinte, on continue simplement d'écouter
                continue
            else:
                print(f"Erreur Kafka: {msg.error()}")
                break
                
        # 4. Désérialisation
        try:
            val_str = msg.value().decode('utf-8')
            data = json.loads(val_str)
            
            # 5. Idempotence : Génération d'un ID unique (Composite Key : Ville + Heure)
            msg_id = f"{data['city']}_{data['timestamp']}"
            
            if msg_id in processed_messages:
                print(f"⚠️ [Doublon ignoré] Donnée déjà traitée : {msg_id}")
            else:
                # Traitement "Métier" de la donnée
                print(f"✅ [Nouveau] {data['city']} : {data['temperature']}°C (Humidité: {data['humidity']}%) - {data['weather']}")
                
                # Enrichissement de la donnée avec des métadonnées système
                data['_metadata'] = {
                    'source': 'api_openweather',
                    'processed_at': datetime.now().isoformat()
                }
                
                # Ajout de la donnée dans le buffer (mémoire tampon)
                message_batch.append(data)
                processed_messages.add(msg_id)
                
                # 6. Traitement par lot (Micro-Batching)
                if len(message_batch) >= BATCH_SIZE:
                    os.makedirs('data_lake/api_meteo', exist_ok=True)
                    
                    # Format demandé : meteo_export_YYYY-MM-DD-HHMM.jsonl
                    date_str = datetime.now().strftime('%Y-%m-%d-%H%M')
                    filename = f"data_lake/api_meteo/meteo_export_{date_str}.jsonl"
                    
                    # Sauvegarde locale des 10 messages d'un coup
                    with open(filename, 'a', encoding='utf-8') as f:
                        for item in message_batch:
                            f.write(json.dumps(item, ensure_ascii=False) + '\n')
                    
                    print(f"📁 [BATCH] {BATCH_SIZE} messages sauvegardés dans {filename}")
                    
                    # 7. Commit manuel groupé (Exactly-Once)
                    # On informe Kafka qu'on a bien traité et sauvegardé ces 10 messages !
                    consumer.commit(asynchronous=False)
                    print(f"🎯 [COMMIT] Offset validé pour le groupe meteo-processor-group.\n")
                    
                    # On vide le buffer pour le prochain lot
                    message_batch.clear()
                
        except json.JSONDecodeError:
            print(f"Erreur de décodage JSON : {msg.value()}")

except KeyboardInterrupt:
    print("\nArrêt manuel du Consumer (Ctrl+C).")
    # En cas d'arrêt brutal, si on a 5 messages en mémoire, on les jette. 
    # Vu qu'on n'a pas fait le commit de ces 5 messages, Kafka nous les redonnera au prochain redémarrage !
    if len(message_batch) > 0:
        print(f"⚠️ {len(message_batch)} messages non-committés seront re-traités au prochain lancement (grâce à l'idempotence et au Kafka Commit).")
finally:
    # Fermeture propre indispensable pour relâcher le Consumer Group sur le serveur
    consumer.close()
    print("Consumer fermé correctement.")
