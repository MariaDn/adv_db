# AdTech Dataset Normalization Project

This repository contains the solution to a data engineering assignment using AdTech dataset

## Project Structure

project/
├── docker-compose.yml

├── sql/
│ ├── V1**create_database.sql
│ └── V2**create_tables.sql
├── etl/
│ ├── data/
│ ├── transform_load.py
│ └── requirements.txt
├── screenshots
├── README.md
└── .gitignore

## How to Run

### 1. Clone the repository

```bash
git clone https://github.com/MariaDn/adv_db
cd adv_db
```

### 2. Configurations
Create a .env file in the project root with the following content:
```bash
# MySQL settings
MYSQL_HOST=mysql
MYSQL_PORT=3306
MYSQL_USER=user
MYSQL_PASSWORD=password
MYSQL_DATABASE=ad_analytics

# MongoDB settings
MONGO_HOST=mongodb
MONGO_PORT=27017

### 3. Start MySQL using Docker and check created tables

```bash
mkdir ./db_data
docker-compose up -d
```

### 4. Start etl process

```bash
docker-compose run --rm etl
```

### 5. Download to mongo and run queries

```bash
docker-compose run --rm mongo_loader
docker-compose run --rm mongo_analytics
```

### 5. Download to casandra and run queries

Add credentials to .env
CASSANDRA_HOST=
CASSANDRA_PORT=
CASSANDRA_USERNAME=
CASSANDRA_PASSWORD=

```bash
docker exec -i ad_analytics_cassandra cqlsh -f /app/schema.cql
docker compose run --rm cassandra_loader
docker compose run --rm cassandra_analytics python /app/cassandra_data/run_queries.py --cutoff-date 2024-10-01
```

### Tech Stack

MySQL 8

Docker

Docker Compose

Python 3.9+

Pip
