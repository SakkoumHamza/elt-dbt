# 🧠 Airbnb ELT Pipeline using Python, PostgreSQL & DBT

This project demonstrates a simple ELT (Extract, Load, Transform) pipeline built with:

- 🐍 Python: to load raw CSV data into PostgreSQL
- 🐘 PostgreSQL: as the data warehouse
- 🧪 DBT: to run SQL transformations 
---

## 📁 Project Structure
```bash

├── airbnb_elt/ # DBT project (models, seeds, etc.)
├── data/ # Raw Airbnb dataset (CSV format)
├── load_csv_to_postgres.py # Python script to load CSV data
├── dbt_project.yml # DBT config file

```

---

## 🚀 How to Run the Project

### 1. Install Prerequisites

Make sure you have installed:

- PostgreSQL 
- Python 3.x
- DBT (`pip install dbt-core dbt-postgres`)

---

### 2. Set Up PostgreSQL

Create a new PostgreSQL database and user. Example:

```sql
CREATE DATABASE airbnb;
CREATE USER postgres WITH ENCRYPTED PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE airbnb TO postgres ;
```

### 3. Load the CSV Data into PostgreSQL
```bash
python load_csv_to_postgres.py
```

### 4. Set up dbt

``` bash
# ~/.dbt/profiles.yml
airbnb_elt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost            # or your DB host
      user: postgres       # your PostgreSQL username
      password: ''    # your PostgreSQL password
      port: 5432
      dbname: airbnb      # name of your PostgreSQL DB
      schema: public             # or another schema if you use one
      threads: 1
```
### 5. Run DBT commands
```bash 
# Run transformation model
dbt run --select airbnb_transformed
```

