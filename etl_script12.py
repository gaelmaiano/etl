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

# --- Logger ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Fonctions ---

def extract_csv(path):
    logging.info("📥 Extraction du CSV...")
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV introuvable : {path}")
    df = pd.read_csv(path)
    logging.info(f"{len(df)} lignes extraites depuis le CSV")
    return df

def extract_sqlite(db_path):
    logging.info("📥 Connexion SQLite...")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DB SQLite introuvable : {db_path}")
    conn = sqlite3.connect(db_path)
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
    data = {}
    for table in tables['name']:
        df = pd.read_sql(f"SELECT * FROM {table}", conn)
        data[table] = df
        logging.info(f"✅ Table '{table}': {len(df)} lignes")
    conn.close()
    return data

def transform_data(df):
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = df[col].astype(int)
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].astype(float)
    return df

def load_to_mysql(df, table_name, engine, index_as_pk=False):
    logging.info(f"📤 Chargement dans MySQL : '{table_name}'")
    dtype_mapping = {}

    for col in df.columns:
        if df[col].dtype == 'object':
            dtype_mapping[col] = types.String(length=255)
        elif df[col].dtype == 'int64':
            dtype_mapping[col] = types.BigInteger()
        elif df[col].dtype == 'float64':
            dtype_mapping[col] = types.Float()
        elif df[col].dtype == 'datetime64[ns]':
            dtype_mapping[col] = types.DateTime()
        else:
            dtype_mapping[col] = None

    if index_as_pk and df.index.name and df.index.name not in df.columns:
        if df.index.dtype == 'int64':
            dtype_mapping[df.index.name] = types.Integer()
        elif df.index.dtype == 'object':
            dtype_mapping[df.index.name] = types.String(length=255)

    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=index_as_pk, dtype=dtype_mapping)
        logging.info(f"✅ {len(df)} lignes insérées dans '{table_name}'")
    except Exception as e:
        logging.error(f"❌ Erreur chargement MySQL [{table_name}]: {e}")
        raise

def main():
    logging.info("🚀 Lancement du script ETL")
    engine = None
    try:
        mysql_url = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        engine = create_engine(mysql_url)

        with engine.connect() as conn:
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            conn.commit()

        df_csv = extract_csv(CSV_PATH)
        sqlite_data = extract_sqlite(SQLITE_DB_PATH)

        # --- Tables SQLite ---
        df_regions = sqlite_data.get('region', pd.DataFrame()).rename(columns={'region_name': 'nom_region'})
        if not df_regions.empty:
            df_regions = df_regions.set_index('region_id')
            df_regions = transform_data(df_regions)
            load_to_mysql(df_regions, 'Regions', engine, index_as_pk=True)

        df_revendeurs = sqlite_data.get('revendeur', pd.DataFrame()).rename(columns={'revendeur_name': 'nom_revendeur'})
        if not df_revendeurs.empty:
            df_revendeurs['email_contact'] = df_revendeurs['nom_revendeur'].apply(lambda x: f"{x.lower().replace(' ', '')}@exemple.com")
            df_revendeurs = df_revendeurs.set_index('revendeur_id')
            df_revendeurs = transform_data(df_revendeurs)
            load_to_mysql(df_revendeurs, 'Revendeurs', engine, index_as_pk=True)

        df_produits = sqlite_data.get('produit', pd.DataFrame()).rename(columns={
            'product_name': 'nom_produit',
            'cout_unitaire': 'prix_unitaire'
        })
        if not df_produits.empty:
            df_produits = df_produits.set_index('product_id')
            df_produits = transform_data(df_produits)
            load_to_mysql(df_produits, 'Produits', engine, index_as_pk=True)

        df_production = sqlite_data.get('production', pd.DataFrame()).rename(columns={
            'quantity': 'quantite_produite',
            'date_production': 'date'
        })
        if not df_production.empty:
            df_production = df_production.set_index('production_id')
            df_production = transform_data(df_production)
            load_to_mysql(df_production, 'Productions', engine, index_as_pk=True)

        # --- Commandes CSV ---
        df_csv = df_csv.rename(columns={
            'numero_commande': 'numero_commande',
            'commande_date': 'date_commande',
            'quantity': 'quantite',
            'unit_price': 'prix_unitaire_vente'
        })

        if 'revendeur_id' in df_csv.columns:
            df_csv['revendeur_id'] = pd.to_numeric(df_csv['revendeur_id'], errors='coerce').fillna(0).astype(int)

        df_csv['commande_id'] = df_csv.groupby(['numero_commande', 'date_commande', 'revendeur_id']).ngroup() + 1
        df_commandes = df_csv[['commande_id', 'numero_commande', 'date_commande', 'revendeur_id']].drop_duplicates()
        df_commandes['date_commande'] = pd.to_datetime(df_commandes['date_commande'])
        df_commandes = df_commandes.set_index('commande_id')
        load_to_mysql(df_commandes, 'Commandes', engine, index_as_pk=True)

        df_lignes = df_csv[['commande_id', 'product_id', 'quantite', 'prix_unitaire_vente']]
        df_lignes['ligne_id'] = range(1, len(df_lignes) + 1)
        df_lignes = df_lignes[['ligne_id', 'commande_id', 'product_id', 'quantite', 'prix_unitaire_vente']]
        df_lignes['quantite'] = pd.to_numeric(df_lignes['quantite'], errors='coerce').fillna(0).astype(int)
        df_lignes['prix_unitaire_vente'] = pd.to_numeric(df_lignes['prix_unitaire_vente'], errors='coerce').fillna(0.0).astype(float)
        df_lignes = df_lignes.set_index('ligne_id')
        load_to_mysql(df_lignes, 'LignesCommande', engine, index_as_pk=True)

        logging.info("✅ ETL terminé avec succès.")

    except FileNotFoundError as e:
        logging.error(f"❌ Fichier manquant : {e}")
    except mysql.connector.Error as e:
        logging.error(f"❌ Erreur MySQL : {e}")
    except Exception as e:
        logging.error(f"❌ Erreur générale : {e}")
    finally:
        if engine:
            try:
                with engine.connect() as conn:
                    conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
                    conn.commit()
                    logging.info("🔒 Clés étrangères réactivées.")
            except Exception as e:
                logging.error(f"Erreur réactivation clés étrangères : {e}")
            engine.dispose()

if __name__ == "__main__":
    main()
