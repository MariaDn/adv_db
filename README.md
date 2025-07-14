# AdTech Dataset Normalization Project

This repository contains the solution to a data engineering assignment using AdTech dataset

## How to Run

### Clone the repository

```bash
git clone https://github.com/MariaDn/adv_db
cd adv_db
```

### Configurations
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

### Start MySQL using Docker and check created tables

```bash
mkdir ./db_data
docker-compose up -d
```

### Start etl process

```bash
docker-compose run --rm etl
```

### Run analytics

```bash
docker-compose run --rm analytics

### Download to mongo and run queries

```bash
docker-compose run --rm mongo_loader
docker-compose run --rm mongo_analytics
```

### Download to casandra and run queries

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

### Add Redis to use API

Add credentials to .env
REDIS_HOST=
REDIS_PORT=

```bash
docker-compose up -d fastapi_api
```

Api docs
http://localhost:8000/docs

Performance can be checked with logs at docker compose logs -f fastapi_api

### Tech Stack

MySQL 8

Docker

Docker Compose

Python 3.9+

Pip
