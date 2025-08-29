
import pandas as pd
import sqlite3
import logging
from sqlalchemy import create_engine, types, text
import os
import mysql.connector

# --- Configuration ---
MYSQL_USER = 'appuser'
MYSQL_PASSWORD = 'example_password'
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3307'
MYSQL_DB = 'distributech_db'

SQLITE_DB_PATH = './data/base_stock.sqlite'
CSV_PATH = 'commande_revendeur_tech_express.csv'
IF_EXISTS_MODE = 'replace'  # ou 'append'

# --- Logger ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_csv(path):
    logging.info("📥 Extraction du fichier CSV...")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Le fichier CSV est introuvable : {path}")
    df = pd.read_csv(path)
    logging.info(f"{len(df)} lignes extraites depuis le CSV.")
    return df

def extract_sqlite(db_path):
    logging.info(f"📥 Connexion à la base SQLite : {db_path}")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Base SQLite introuvable : {db_path}")
    conn = sqlite3.connect(db_path)
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
    data = {}
    for table in tables['name']:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        data[table] = df
        logging.info(f"Table '{table}' : {len(df)} lignes extraites")
    conn.close()
    return data

def transform_data(df):
    logging.info(f"🔧 Transformation générique des données ({len(df)} lignes)...")
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = df[col].astype('int')
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].astype('float')
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna('').astype(str)
    return df

def load_to_mysql(df, table_name, engine, index_as_pk=False):
    logging.info(f"💾 Chargement dans MySQL : table '{table_name}'")
    
    if index_as_pk and df.index.name and df.index.name not in df.columns:
        df.reset_index(inplace=True)
    
    dtype_mapping = {}
    for col in df.columns:
        if df[col].dtype == 'object':
            dtype_mapping[col] = types.String(length=255)
        elif df[col].dtype == 'int64':
            dtype_mapping[col] = types.Integer()
        elif df[col].dtype == 'float64':
            dtype_mapping[col] = types.Float()
        elif df[col].dtype.name.startswith('datetime'):
            dtype_mapping[col] = types.DateTime()
    
    try:
        df.to_sql(table_name, con=engine, if_exists=IF_EXISTS_MODE, index=index_as_pk, dtype=dtype_mapping)
        logging.info(f"✅ {len(df)} lignes insérées dans '{table_name}'")
    except Exception as e:
        logging.error(f"❌ Erreur dans le chargement de '{table_name}' : {e}")
        raise

def main():
    logging.info("🚀 Démarrage du processus ETL")
    engine = None
    try:
        mysql_url = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        engine = create_engine(mysql_url)

        with engine.connect() as connection:
            logging.info("🔐 Connexion MySQL établie")
            connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            connection.commit()

        # --- Extraction ---
        df_csv = extract_csv(CSV_PATH)
        sqlite_data = extract_sqlite(SQLITE_DB_PATH)

        # --- Regions ---
        df_regions = sqlite_data.get('region', pd.DataFrame()).rename(columns={'region_name': 'nom_region'})
        if not df_regions.empty:
            df_regions = df_regions.set_index('region_id')
            load_to_mysql(transform_data(df_regions), 'Regions', engine, index_as_pk=True)

        # --- Revendeurs ---
        df_revendeurs = sqlite_data.get('revendeur', pd.DataFrame()).rename(columns={'revendeur_name': 'nom_revendeur'})
        if not df_revendeurs.empty:
            df_revendeurs['email_contact'] = df_revendeurs['nom_revendeur'].apply(lambda x: f"{x.lower().replace(' ', '')}@exemple.com")
            df_revendeurs = df_revendeurs.set_index('revendeur_id')
            load_to_mysql(transform_data(df_revendeurs), 'Revendeurs', engine, index_as_pk=True)

        # --- Produits ---
        df_prod_
