# 🚀 Real-Time Data Streaming Pipeline (Apache Kafka)

## 📌 Description du Projet
Ce projet académique démontre la mise en place d'une architecture de **Stream Processing** complète et robuste, utilisant **Apache Kafka** en mode KRaft. 

L'objectif principal est de construire un pipeline de données en temps réel capable d'ingérer, de traiter et de stocker des flux de données provenant de deux sources distinctes (une API externe et une Base de Données relationnelle), en appliquant les meilleures pratiques de l'ingénierie des données (Idempotence, Change Data Capture, Micro-Batching, sémantique Exactly-Once).

## 🏗️ Architecture Technique

```mermaid
flowchart LR
    subgraph Sources [Data Sources]
        API([API OpenWeatherMap])
        DB[(PostgreSQL)]
    end

    subgraph Producers [Data Ingestion]
        P_API{{Producer API}}
        P_DB{{Producer DB}}
    end

    subgraph Streaming [Apache Kafka Cluster]
        T_API[(Topic: api_meteo)]
        T_DB[(Topic: db_trafic)]
    end

    subgraph Consumers [Data Processing]
        C_API{{Consumer API}}
        C_DB{{Consumer DB}}
    end

    subgraph Storage [Raw Data Lake]
        DL_API[(JSONL: Météo)]
        DL_DB[(JSONL: Trafic)]
    end

    %% API Flow
    API -- "HTTP GET (60s)" --> P_API
    P_API -- "JSON / Bytes" --> T_API
    T_API -- "Subscribe" --> C_API
    C_API -- "Idempotence / Micro-Batch" --> DL_API

    %% DB Flow
    DB -- "SQL Incremental Load" --> P_DB
    P_DB -- "JSON / Bytes" --> T_DB
    T_DB -- "Subscribe" --> C_DB
    C_DB -- "Idempotence / Micro-Batch" --> DL_DB

    %% Styling
    classDef source fill:#e3f2fd,stroke:#1e88e5,stroke-width:2px,color:#0d47a1;
    classDef python fill:#e8f5e9,stroke:#43a047,stroke-width:2px,color:#1b5e20;
    classDef kafka fill:#fff3e0,stroke:#fb8c00,stroke-width:2px,color:#e65100;
    classDef lake fill:#f3e5f5,stroke:#8e24aa,stroke-width:2px,color:#4a148c;

    class API,DB source;
    class P_API,P_DB,C_API,C_DB python;
    class T_API,T_DB kafka;
    class DL_API,DL_DB lake;
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
