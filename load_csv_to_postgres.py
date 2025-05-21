import pandas as pd
import psycopg2

"""Module for loading Airbnb data from CSV into PostgreSQL database."""

# === Configuration de la base ===
DB_CONFIG = {
    'dbname': 'airbnb',
    'user': 'postgres',
    'password': '',
    'host': 'localhost',
    'port': 5432
}

# === Chemin vers le CSV ===
CSV_FILE = "data/Airbnb_Open_Data.csv"

# === Nom de la table cible ===
TABLE_NAME = 'airbnb_data'

def load_csv_to_postgres():
    try:
        # Lire le CSV avec pandas
        df = pd.read_csv(CSV_FILE)

        # Connexion à PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)

        # Cursor object
        cursor = conn.cursor()

        cursor.execute(f""" CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                            "id" BIGINT ,
                            "name" TEXT,
                            "host id" BIGINT,
                            "host_identity_verified" TEXT,
                            "host name" TEXT,
                            "neighbourhood group" TEXT,
                            "neighbourhood" TEXT,
                            "lat" NUMERIC,
                            "long" NUMERIC,
                            "country" TEXT,
                            "country code" TEXT,
                            "instant_bookable" BOOLEAN,
                            "cancellation_policy" TEXT,
                            "room type" TEXT,
                            "Construction year" NUMERIC,
                            "price" TEXT,
                            "service fee" TEXT,
                            "minimum nights" NUMERIC,
                            "number of reviews" NUMERIC,
                            "last review" DATE,
                            "reviews per month" NUMERIC,
                            "review rate number" NUMERIC,
                            "calculated host listings count" NUMERIC,
                            "availability 365" NUMERIC,
                            "house_rules" TEXT,
                            "license" TEXT  
                       );
                    """)
        conn.commit()
        

        # Enregistrer temporairement le CSV pour COPY
        temp_csv = "/tmp/temp_load.csv"
        df.to_csv(temp_csv, index=False, header=False)

        # Charger le fichier via COPY
        with open(temp_csv, 'r') as f:
            cursor.copy_expert(f"COPY {TABLE_NAME} FROM STDIN WITH CSV", f)

        conn.commit()
        cursor.close()
        conn.close()

        print("✅🦁 Données chargées avec succès dans PostgreSQL.")

    except Exception as e:
        print("❌ Erreur lors du chargement :", e)

if __name__ == "__main__":
    load_csv_to_postgres()
