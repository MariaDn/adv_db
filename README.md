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

### 2. Start MySQL using Docker and check created tables

```bash
mkdir ./db_data
docker-compose up -d
```

### 3. Start etl process

```bash
docker-compose run --rm etl
```

### 4. Tech Stack

MySQL 8

Docker

Docker Compose

Python 3.9+

Pip
