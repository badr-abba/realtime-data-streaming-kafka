# 🚀 Real-Time Data Streaming Pipeline (Apache Kafka)

## 📌 Description du Projet
Ce projet académique démontre la mise en place d'une architecture de **Stream Processing** complète et robuste, utilisant **Apache Kafka** en mode KRaft. 

L'objectif principal est de construire un pipeline de données en temps réel capable d'ingérer, de traiter et de stocker des flux de données provenant de deux sources distinctes (une API externe et une Base de Données relationnelle), en appliquant les meilleures pratiques de l'ingénierie des données (Idempotence, Change Data Capture, Micro-Batching, sémantique Exactly-Once).

## 🏗️ Architecture Technique

```mermaid
graph TD
    %% Sources
    API[API OpenWeatherMap]
    DB[(PostgreSQL<br>trafic_bus)]

    %% Producers
    P_API[Producer API<br>Python]
    P_DB[Producer DB<br>Python]

    %% Kafka
    subgraph Kafka_Cluster [Cluster Apache Kafka]
        T_API(Topic:<br>topic_api_meteo)
        T_DB(Topic:<br>topic_db_trafic)
    end

    %% Consumers
    C_API[Consumer API<br>Python]
    C_DB[Consumer DB<br>Python]

    %% Data Lake
    subgraph Data_Lake [Data Lake Local]
        DL_API[(Export JSONL<br>Météo)]
        DL_DB[(Export JSONL<br>Trafic)]
    end

    %% Flux Météo
    API -- Polling (60s) --> P_API
    P_API -- Produce --> T_API
    T_API -- Consume --> C_API
    C_API -- Micro-Batch (10) --> DL_API

    %% Flux DB
    DB -- Incremental Load<br>(Watermark) --> P_DB
    P_DB -- Produce --> T_DB
    T_DB -- Consume --> C_DB
    C_DB -- Micro-Batch (5) --> DL_DB

    %% Styles
    classDef source fill:#fce4ec,stroke:#f06292,stroke-width:2px;
    classDef kafka fill:#fff9c4,stroke:#fbc02d,stroke-width:2px;
    classDef lake fill:#e3f2fd,stroke:#64b5f6,stroke-width:2px;
    classDef python fill:#e8f5e9,stroke:#81c784,stroke-width:2px;

    class API,DB source;
    class T_API,T_DB kafka;
    class DL_API,DL_DB lake;
    class P_API,P_DB,C_API,C_DB python;
```

## 🛠️ Stack Technologique
- **Message Broker** : Apache Kafka 3.7 (KRaft Mode)
- **Base de Données Source** : PostgreSQL 15
- **Langage** : Python 3
- **Déploiement** : Docker & Docker Compose
- **Monitoring** : Kafbat UI
- **Stockage cible** : Format JSON Lines (Data Lake)

## ⚙️ Concepts Clés Implémentés
1. **Infrastructure as Code (IaC)** : Déploiement automatisé du cluster et création idempotente des topics via *Init Containers*.
2. **Incremental Load (Polling)** : Extraction base de données basée sur un curseur (High-Water Mark) évitant les doublons à la source.
3. **Partitionnement Strict** : Utilisation des Clés Primaires comme clés de partition Kafka garantissant l'ordre chronologique de livraison.
4. **Idempotence & Exactly-Once** : Gestion d'un registre en mémoire (Set) par les consommateurs croisé avec un *Commit Différé* manuel de Kafka.
5. **Micro-Batching** : Tampon mémoire (Buffer) côté consommateur pour minimiser les I/O disques lors de l'écriture en zone Raw.

## 🚀 Guide de Démarrage

### 1. Prérequis
- Docker Desktop (ou Docker Engine via WSL2)
- Python 3.x
- Clé d'API OpenWeatherMap

### 2. Installation
Clonez le dépôt, puis configurez l'environnement virtuel :
```bash
python -m venv venv
# Windows : venv\Scripts\activate
# Linux/WSL : source venv/bin/activate
pip install -r requirements.txt
```

Créez un fichier `.env` à la racine :
```env
OPENWEATHER_API_KEY=votre_cle_api
DB_USER=postgres
DB_PASSWORD=votre_mdp
DB_HOST=localhost
DB_PORT=5432
DB_NAME=transport_meteo
KAFKA_BROKER=localhost:9092
```

### 3. Lancement de l'Infrastructure
Démarrez les conteneurs en tâche de fond :
```bash
docker compose up -d
```
*L'interface de surveillance Kafbat UI sera disponible sur `http://localhost:8080`.*

### 4. Exécution du Pipeline Temps Réel
Dans des terminaux séparés, lancez les différents micro-services :

**Flux Météo :**
```bash
python producer_api.py
python consumer_api.py
```

**Flux Trafic (PostgreSQL) :**
```bash
python producer_db.py
python consumer_db.py
```

Les données consolidées apparaîtront en temps réel dans le répertoire `/data_lake`.
