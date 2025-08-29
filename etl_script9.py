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

# CHEMINS CORRIGÉS : Assurez-vous que ces chemins correspondent à l'emplacement de vos fichiers
SQLITE_DB_PATH = './data/base_stock.sqlite'
CSV_PATH = 'commande_revendeur_tech_express.csv'

# --- Configuration du logger ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_csv(path):
    """Extrait les données d'un fichier CSV. Ajout d'une vérification d'existence."""
    logging.info("Extraction CSV...")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Le fichier CSV n'existe pas : {path}")
    df = pd.read_csv(path)
    logging.info(f"{len(df)} lignes extraites depuis le CSV")
    return df

def extract_sqlite(db_path):
    """Extrait les données de toutes les tables d'une base de données SQLite. Ajout d'une vérification d'existence."""
    logging.info(f"Connexion à SQLite : {db_path}...")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"La base de données SQLite n'existe pas : {db_path}")
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
    """Effectue des transformations génériques sur un DataFrame."""
    logging.info(f"Transformation des données pour {len(df)} lignes...")
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = df[col].astype('int')
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].astype('float')
    return df

def load_to_mysql(df, table_name, engine, index_as_pk=False):
    """
    Charge un DataFrame dans une table MySQL spécifique.
    index_as_pk=True indique que l'index du DataFrame doit être traité comme clé primaire.
    """
    logging.info(f"Chargement dans MySQL : table '{table_name}'...")
    
    dtype_mapping = {}
    # S'assurer que les types sont correctement mappés, y compris pour les colonnes d'index
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

    # Si l'index doit être une PK, s'assurer que son type est également mappé
    if index_as_pk and df.index.name is not None and df.index.name not in dtype_mapping:
         if df.index.dtype == 'int64':
             dtype_mapping[df.index.name] = types.BigInteger()
         elif df.index.dtype == 'object':
             dtype_mapping[df.index.name] = types.String(length=255)


    try:
        # Utilisez 'index=index_as_pk' pour gérer la création de la clé primaire
        df.to_sql(table_name, con=engine, if_exists='replace', index=index_as_pk, dtype=dtype_mapping)
        logging.info(f"{len(df)} lignes insérées dans la table '{table_name}'")
    except Exception as e:
        logging.error(f"Erreur pendant le chargement dans la table '{table_name}' : {e}")
        raise

def main():
    logging.info("🚀 Démarrage du processus ETL")
    engine = None
    try:
        mysql_url = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        engine = create_engine(mysql_url)
        
        with engine.connect() as connection:
            logging.info("Connexion à la base de données MySQL établie.")
            logging.info("Désactivation des vérifications de clés étrangères...")
            connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            connection.commit()
            logging.info("Vérifications de clés étrangères désactivées.")

        df_csv = extract_csv(CSV_PATH)
        sqlite_data = extract_sqlite(SQLITE_DB_PATH)

        # --- Transformation et Chargement ---
        # Définir l'ID comme index et le charger comme PK
        
        # Régions
        df_regions = sqlite_data.get('region', pd.DataFrame()).rename(columns={'region_name': 'nom_region'})
        if not df_regions.empty:
            df_regions = df_regions.set_index('region_id') # Définir 'region_id' comme index
            load_to_mysql(df_regions, 'Regions', engine, index_as_pk=True)

        # Revendeurs
        df_revendeurs = sqlite_data.get('revendeur', pd.DataFrame()).rename(columns={'revendeur_name': 'nom_revendeur'})
        df_revendeurs['email_contact'] = df_revendeurs['nom_revendeur'].apply(lambda x: f"{x.lower().replace(' ', '')}@exemple.com")
        if not df_revendeurs.empty:
            df_revendeurs = df_revendeurs.set_index('revendeur_id') # Définir 'revendeur_id' comme index
            load_to_mysql(df_revendeurs, 'Revendeurs', engine, index_as_pk=True)

        # Produits
        df_produits = sqlite_data.get('produit', pd.DataFrame()).rename(columns={'product_name': 'nom_produit', 'cout_unitaire': 'prix_unitaire'})
        if not df_produits.empty:
            df_produits = df_produits.set_index('product_id') # Définir 'product_id' comme index
            load_to_mysql(df_produits, 'Produits', engine, index_as_pk=True)
            
        # Production
        df_production = sqlite_data.get('production', pd.DataFrame()).rename(columns={'quantity': 'quantite_produite', 'date_production': 'date'})
        if not df_production.empty:
             df_production = df_production.set_index('production_id') # Définir 'production_id' comme index
             load_to_mysql(df_production, 'Productions', engine, index_as_pk=True)

        # Commandes (à partir du CSV)
        df_commandes_csv = df_csv.rename(columns={
            'numero_commande': 'numero_commande',
            'commande_date': 'date_commande',
            'quantity': 'quantite',
            'unit_price': 'prix_unitaire_vente'
        })
        
        if 'revendeur_id' in df_commandes_csv.columns:
            df_commandes_csv['revendeur_id'] = pd.to_numeric(df_commandes_csv['revendeur_id'], errors='coerce').fillna(0).astype(int)
        
        # Créer un ID unique pour chaque commande
        df_commandes_csv['commande_id'] = df_commandes_csv.groupby(['numero_commande', 'date_commande', 'revendeur_id']).ngroup() + 1
        
        df_commandes = df_commandes_csv[['commande_id', 'numero_commande', 'date_commande', 'revendeur_id']].drop_duplicates()
        df_commandes['date_commande'] = pd.to_datetime(df_commandes['date_commande'])
        if not df_commandes.empty:
            df_commandes = df_commandes.set_index('commande_id') # Définir 'commande_id' comme index
            load_to_mysql(df_commandes, 'Commandes', engine, index_as_pk=True)

        # Lignes de Commande (à partir du CSV)
        df_lignes_commande = df_commandes_csv[['commande_id', 'product_id', 'quantite', 'prix_unitaire_vente']]
        df_lignes_commande['ligne_id'] = range(1, len(df_lignes_commande) + 1)
        
        df_lignes_commande = df_lignes_commande[['ligne_id', 'commande_id', 'product_id', 'quantite', 'prix_unitaire_vente']]
        
        df_lignes_commande['quantite'] = pd.to_numeric(df_lignes_commande['quantite'], errors='coerce').fillna(0).astype(int)
        df_lignes_commande['prix_unitaire_vente'] = pd.to_numeric(df_lignes_commande['prix_unitaire_vente'], errors='coerce').fillna(0.0).astype(float)

        if not df_lignes_commande.empty:
            df_lignes_commande = df_lignes_commande.set_index('ligne_id') # Définir 'ligne_id' comme index
            load_to_mysql(df_lignes_commande, 'LignesCommande', engine, index_as_pk=True)

        logging.info("Processus ETL terminé avec succès.")

    except FileNotFoundError as e:
        logging.error(f"❌ Échec du processus ETL : Fichier non trouvé - {e}")
    except mysql.connector.Error as e:
        logging.error(f"❌ Échec du processus ETL (Erreur MySQL) : {e}")
    except Exception as e:
        logging.error(f"❌ Échec du processus ETL : {e}")
    finally:
        if engine:
            try:
                with engine.connect() as connection:
                    logging.info("Réactivation des vérifications de clés étrangères...")
                    connection.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
                    connection.commit()
                    logging.info("Vérifications de clés étrangères réactivées.")
            except Exception as e:
                logging.error(f"Erreur lors de la réactivation des clés étrangères : {e}")
            finally:
                engine.dispose()

if __name__ == "__main__":
    main()