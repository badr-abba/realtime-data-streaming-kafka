import os
import json
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

# Charger les variables d'environnement
load_dotenv()
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = "topic_db_trafic"

# Configurer le Consumer Kafka
consumer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'trafic-processor-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}
consumer = Consumer(consumer_config)
consumer.subscribe([TOPIC_NAME])

# Registre d'idempotence (Evite les doublons)
processed_messages = set()

# Batch Processing
message_batch = []
BATCH_SIZE = 5  # Lot plus petit pour voir les resultats plus vite

print(f"Demarrage du Consumer DB Trafic sur {TOPIC_NAME} (Lot de {BATCH_SIZE} messages)...")
print("En attente de messages (Ctrl+C pour arreter)\n")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Erreur Kafka: {msg.error()}")
                break
            continue
                
        try:
            val_str = msg.value().decode('utf-8')
            data = json.loads(val_str)
            
            # Idempotence basee sur l'ID (Cle primaire) de la base de donnees
            msg_id = str(data['id'])
            
            if msg_id in processed_messages:
                print(f"[Ignore] Message en double (ID: {msg_id})")
                continue
                
            print(f"[Lu] Trafic ID {data['id']} : Bus {data['ligne']} a {data['arret']} avec {data['nombre_passagers']} passagers")
            
            # Enrichissement Metadata
            data['_metadata'] = {
                'source': 'postgres_transport',
                'processed_at': datetime.now().isoformat()
            }
            
            message_batch.append(data)
            processed_messages.add(msg_id)
            
            if len(message_batch) >= BATCH_SIZE:
                os.makedirs('data_lake/db_trafic', exist_ok=True)
                
                date_str = datetime.now().strftime('%Y-%m-%d-%H%M')
                filename = f"data_lake/db_trafic/trafic_export_{date_str}.jsonl"
                
                with open(filename, 'a', encoding='utf-8') as f:
                    for item in message_batch:
                        f.write(json.dumps(item, ensure_ascii=False) + '\n')
                
                print(f"[Batch] {BATCH_SIZE} enregistrements de trafic sauvegardes dans {filename}")
                
                # Commit Kafka EXACTLY ONCE
                consumer.commit(asynchronous=False)
                print("[Commit] Offset du groupe trafic valide.\n")
                
                message_batch.clear()
                
        except json.JSONDecodeError:
            print("Erreur de decodage JSON.")

except KeyboardInterrupt:
    print("\nArret manuel en cours...")
    if message_batch:
        print(f"Attention : {len(message_batch)} messages non committes seront retraites plus tard.")
finally:
    consumer.close()
    print("Consumer trafic arrete correctement.")
