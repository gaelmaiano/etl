
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
import pandas as pd
import sqlite3
import logging
from sqlalchemy import create_engine, types, text
import os
import mysql.connector

# --- Configuration MySQL ---
MYSQL_USER = 'appuser'
MYSQL_PASSWORD = 'example_password'
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3307'
MYSQL_DB = 'distributech_db'

# --- Chemins fichiers ---
SQLITE_DB_PATH = './data/base_stock.sqlite'
CSV_PATH = 'commande_revendeur_tech_express.csv'

# --- Configuration logger ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Fonctions ETL ---
def extract_csv(path):
    logging.info("Extraction CSV...")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Fichier CSV introuvable : {path}")
    df = pd.read_csv(path)
    logging.info(f"{len(df)} lignes extraites depuis le CSV")
    return df

def extract_sqlite(db_path):
    logging.info(f"Connexion à SQLite : {db_path}")
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
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = df[col].astype(int)
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].astype(float
