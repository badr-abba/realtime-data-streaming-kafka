import os
import json
import time
import psycopg2
import schedule
from dotenv import load_dotenv
from confluent_kafka import Producer

# Charger les variables d'environnement
load_dotenv()
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')

WATERMARK_FILE = 'data_lake/db_watermark.txt'
TOPIC_NAME = 'topic_db_trafic'

# Configurer le Producer Kafka
producer_config = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'python-db-producer'
}
producer = Producer(producer_config)

def delivery_report(err, msg):
    """Callback execute apres chaque tentative d'envoi vers Kafka."""
    if err is not None:
        print(f"Echec de l'envoi Kafka : {err}")
    else:
        print(f"Message Kafka envoye sur {msg.topic()} [Offset: {msg.offset()}]")

def get_db_connection():
    try:
        return psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
    except Exception as e:
        print(f"Erreur de connexion DB : {e}")
        return None

def get_watermark():
    """Lit le dernier ID traite depuis le fichier local."""
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE, 'r') as f:
            content = f.read().strip()
            return int(content) if content.isdigit() else 0
    return 0

def save_watermark(last_id):
    """Sauvegarde le nouvel ID maximum traite."""
    os.makedirs(os.path.dirname(WATERMARK_FILE), exist_ok=True)
    with open(WATERMARK_FILE, 'w') as f:
        f.write(str(last_id))

def fetch_trafic_data():
    watermark = get_watermark()
    print(f"\n--- [PostgreSQL] Extraction Incrementale (Watermark: {watermark}) - {time.strftime('%H:%M:%S')} ---")
    
    conn = get_db_connection()
    if not conn:
        return

    try:
        cursor = conn.cursor()
        
        # Incremental Load : on ne lit que ce qui est superieur au watermark
        query = """
            SELECT t.id, t.ligne, t.arret, t.date, t.nombre_passagers, 
                   m.temperature, m.pluie, m.description
            FROM trafic_bus t
            JOIN meteo m ON t.date = m.date
            WHERE t.id > %s
            ORDER BY t.id ASC
        """
        
        cursor.execute(query, (watermark,))
        rows = cursor.fetchall()
        
        if not rows:
            print("Aucune nouvelle donnee trouvee.")
        else:
            print(f"{len(rows)} nouvelles lignes extraites. Envoi vers Kafka...")
            max_id = watermark
            
            for row in rows:
                current_id = row[0]
                
                # Securite pour gerer les objets Date de Python en JSON
                date_str = row[3].isoformat() if hasattr(row[3], 'isoformat') else str(row[3])
                
                # Formatage de la donnee metier en JSON
                payload = {
                    "id": current_id,
                    "ligne": row[1],
                    "arret": row[2],
                    "date": date_str,
                    "nombre_passagers": row[4],
                    "meteo_temperature": row[5],
                    "meteo_pluie": row[6],
                    "meteo_description": row[7]
                }
                
                # Conversion en binaire UTF-8
                json_data = json.dumps(payload).encode('utf-8')
                
                # On utilise l'ID comme cle pour garantir un ordre d'arrivee strict dans Kafka
                byte_key = str(current_id).encode('utf-8')
                
                # Envoi asynchrone a Kafka
                producer.produce(
                    topic=TOPIC_NAME,
                    key=byte_key,
                    value=json_data,
                    callback=delivery_report
                )
                
                # Depiler les accuses de reception en arriere-plan
                producer.poll(0)
                
                if current_id > max_id:
                    max_id = current_id
                    
            # Mettre a jour le registre (Uniquement apres que la lecture DB soit un succes)
            save_watermark(max_id)
            print(f"[Watermark] Curseur mis a jour a l'ID {max_id}")
            
    except Exception as e:
        print(f"Erreur lors de l'extraction : {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    print(f"Demarrage du Producer DB vers {KAFKA_BROKER} (Topic: {TOPIC_NAME})...")
    
    # Executer immediatement au lancement
    fetch_trafic_data()
    
    # Planifier toutes les 30 secondes
    schedule.every(30).seconds.do(fetch_trafic_data)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nArret manuel du script. Purge des messages Kafka en attente...")
        producer.flush()
        print("Fin du programme.")