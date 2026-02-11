# 🚀 Projet Big Data – Architecture en Médaillon
## 🎯 Pilotage de la performance commerciale & optimisation des revenus e-commerce

---

## 📌 1. Contexte du projet

De nombreux entrepreneurs e-commerce rencontrent un problème majeur :

- 📉 Chiffre d’affaires faible
- 📦 Mauvaise gestion des stocks
- ⭐ Impact des avis mal mesuré
- 📊 Manque de pilotage par la donnée

Ce projet met en place une architecture **Big Data en médaillon (Bronze / Silver / Gold)** pour aider à la prise de décision.

---

## 🏗️ 2. Architecture

CSV + SQLite
↓
BRONZE (HDFS - Raw)
↓
SILVER (Spark + Hive)
↓
GOLD (PostgreSQL - Datamart)
↓
API REST (FastAPI)
↓
Power BI



---

## 🧱 3. Stack technique

| Couche   | Technologie |
|-----------|------------|
| Orchestration | Docker / Docker Compose |
| Raw Storage   | HDFS |
| Processing    | Spark |
| Métastore     | Hive |
| Datamart      | PostgreSQL |
| API           | FastAPI |
| BI            | Power BI |

---


---

# 🐳 4. Lancer l'infrastructure

```bash
docker-compose up -d

5. Couche Bronze
▶ Ingestion 

docker exec -it spark-master bash -lc "/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--jars /jars/sqlite-jdbc.jar \
/project/spark/feeder.py"


🥈 6. Couche Silver
▶ Traitement

docker exec -it spark-master bash -lc "/spark/bin/spark-submit \
--master spark://spark-master:7077 \
/project/spark/processor.py"


▶ Création tables Hive

docker exec -it hive-server beeline \
-u "jdbc:hive2://hive-server:10000/default" \
-f /project/sql/hive_silver_ddl.sql


🥇 7. Couche Gold (Datamart)

▶ Génération Gold

docker exec -it postgres-gold psql -U gold_user -d gold -c "CREATE SCHEMA IF NOT EXISTS datamart;"

docker exec -it spark-master bash -lc "/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--jars /jars/postgresql-jdbc.jar \
/project/spark/datamart.py"


🌐 8. API REST

Lancer l’API :

docker-compose build api

docker-compose up -d api

Documentation Swagger :

http://localhost:8000/docs




