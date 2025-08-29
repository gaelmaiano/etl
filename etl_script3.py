import pandas as pd
import sqlite3
import logging
from sqlalchemy import create_engine, types
import os

# --- Configuration ---
MYSQL_USER = 'appuser'
MYSQL_PASSWORD = 'example_password'
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3307'
MYSQL_DB = 'distributech_db'

SQLITE_DB_PATH = 'local.sqlite'
CSV_PATH = 'data.csv'

# --- Logger setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_csv(path):
    logging.info("Extraction CSV...")
    df = pd.read_csv(path)
    logging.info(f"{len(df)} lignes extraites depuis le CSV")
    return df

def extract_sqlite(db_path):
    logging.info("Connexion à SQLite...")
    conn = sqlite3.connect(db_path)
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
    data = {}
    for table in tables['name']:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        data[table] = df
        logging.info(f"{table} : {len(df)} lignes extraites")
    conn.close()
    return data

def transform_data(df):
    logging.info("Transformation des données...")
    # Exemple de conversion : forcer les types simples pour MySQL
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = df[col].astype('int')
    return df

def load_to_mysql(df, table_name, engine):
    logging.info(f"Chargement dans MySQL : {table_name}")
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False,
                  dtype={col: types.String() if df[col].dtype == object else None for col in df.columns})
        logging.info(f"{len(df)} lignes insérées dans {table_name}")
    except Exception as e:
        logging.error(f"Erreur pendant le chargement dans {table_name} : {e}")
        raise

def main():
    logging.info("🚀 Démarrage du processus ETL")
    try:
        # Connexion MySQL
        mysql_url = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        engine = create_engine(mysql_url)

        # --- Extraction ---
        df_csv = extract_csv(CSV_PATH)
        sqlite_data = extract_sqlite(SQLITE_DB_PATH)

        # --- Transformation ---
        df_csv = transform_data(df_csv)
        sqlite_data = {name: transform_data(df) for name, df in sqlite_data.items()}

        # --- Chargement ---
        load_to_mysql(df_csv, 'csv_data', engine)  # nom de table à adapter
        for name, df in sqlite_data.items():
            load_to_mysql(df, name, engine)

        logging.info("✅ ETL terminé avec succès")

    except Exception as e:
        logging.error(f"❌ Échec du processus ETL : {e}")

if __name__ == '__main__':
    main()
