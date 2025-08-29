import pandas as pd
import sqlite3
import logging
from sqlalchemy import create_engine, types, text # Import de 'text' pour les requêtes brutes
import os
import mysql.connector # Ajout pour gérer les erreurs de connexion MySQL

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
# Ces lignes DOIVENT être au début du script
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
    # Cette boucle est utile pour s'assurer que les types sont compatibles avant le chargement
    for col in df.select_dtypes(include=['int64']).columns:
        df[col] = df[col].astype('int')
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].astype('float')
    # Vous pouvez ajouter d'autres transformations spécifiques à vos données ici
    return df

def load_to_mysql(df, table_name, engine):
    """Charge un DataFrame dans une table MySQL spécifique avec un mappage de type explicite."""
    logging.info(f"Chargement dans MySQL : table '{table_name}'...")
    
    # Mapper les types de colonnes pandas vers les types SQLAlchemy/MySQL
    dtype_mapping = {}
    for col in df.columns:
        if df[col].dtype == 'object':
            # Utiliser String avec une longueur appropriée pour les chaînes de caractères
            dtype_mapping[col] = types.String(length=255) 
        elif df[col].dtype == 'int64':
            # Utiliser BigInteger pour les entiers 64 bits de pandas
            dtype_mapping[col] = types.BigInteger()
        elif df[col].dtype == 'float64':
            # Utiliser Float pour les nombres flottants 64 bits
            dtype_mapping[col] = types.Float()
        elif df[col].dtype == 'datetime64[ns]':
            # Utiliser DateTime pour les dates/heures
            dtype_mapping[col] = types.DateTime()
        else:
            # Laisser SQLAlchemy inférer les autres types si non spécifiés
            dtype_mapping[col] = None

    try:
        # 'if_exists='replace'' vide et recharge la table. Changez en 'append' pour ajouter des données.
        df.to_sql(table_name, con=engine, if_exists='replace', index=False, dtype=dtype_mapping)
        logging.info(f"{len(df)} lignes insérées dans la table '{table_name}'")
    except Exception as e:
        logging.error(f"Erreur pendant le chargement dans la table '{table_name}' : {e}")
        raise # Rélève l'exception pour qu'elle soit capturée par le bloc principal

def main():
    logging.info("🚀 Démarrage du processus ETL")
    engine = None # Initialiser l'engine à None pour le bloc finally
    try:
        # Connexion MySQL
        mysql_url = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        engine = create_engine(mysql_url)
        
        with engine.connect() as connection:
            logging.info("Connexion à la base de données MySQL établie.")
            
            # --- Désactiver temporairement les vérifications de clés étrangères ---
            logging.info("Désactivation des vérifications de clés étrangères...")
            connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;")) # Utiliser text() avec SQLAlchemy 2.0+
            connection.commit() # Important pour que le changement soit appliqué
            logging.info("Vérifications de clés étrangères désactivées.")

        # --- Extraction ---
        df_csv = extract_csv(CSV_PATH)
        sqlite_data = extract_sqlite(SQLITE_DB_PATH)

        # --- Transformation et Chargement ---
        # Renommage des colonnes: ne plus renommer les colonnes d'ID pour qu'elles correspondent aux FK
        
        # Données des régions: Garder 'region_id', renommer 'region_name'
        df_regions = sqlite_data.get('region', pd.DataFrame()).rename(columns={'region_name': 'nom_region'})
        if not df_regions.empty:
            load_to_mysql(df_regions, 'Regions', engine)

        # Données des revendeurs: Garder 'revendeur_id' et 'region_id', renommer 'revendeur_name'
        df_revendeurs = sqlite_data.get('revendeur', pd.DataFrame()).rename(columns={'revendeur_name': 'nom_revendeur'})
        df_revendeurs['email_contact'] = df_revendeurs['nom_revendeur'].apply(lambda x: f"{x.lower().replace(' ', '')}@exemple.com")
        if not df_revendeurs.empty:
            load_to_mysql(df_revendeurs, 'Revendeurs', engine)

        # Données des produits: Garder 'product_id', renommer 'product_name' et 'cout_unitaire'
        df_produits = sqlite_data.get('produit', pd.DataFrame()).rename(columns={'product_name': 'nom_produit', 'cout_unitaire': 'prix_unitaire'})
        if not df_produits.empty:
            load_to_mysql(df_produits, 'Produits', engine)
            
        # Données de production: Garder 'production_id' et 'product_id', renommer 'quantity' et 'date_production'
        df_production = sqlite_data.get('production', pd.DataFrame()).rename(columns={'quantity': 'quantite_produite', 'date_production': 'date'})
        if not df_production.empty:
             load_to_mysql(df_production, 'Productions', engine)

        # Transformation et chargement des données CSV (commandes)
        # Ici, nous ne renommons que les colonnes qui ne sont PAS des ID (clés primaires/étrangères)
        df_commandes_csv = df_csv.rename(columns={
            # Ces colonnes sont déjà nommées correctement si elles correspondent au CSV source
            'numero_commande': 'numero_commande', # Pas de changement si le nom est le même
            'commande_date': 'date_commande',     # Renommé pour correspondre à un style cohérent
            # 'revendeur_id' reste 'revendeur_id' (si c'est le nom dans le CSV et FK)
            # 'region_id' reste 'region_id' (si c'est le nom dans le CSV et FK)
            # 'product_id' reste 'product_id' (si c'est le nom dans le CSV et FK)
            'quantity': 'quantite',               # Renommé pour correspondre à un style cohérent
            'unit_price': 'prix_unitaire_vente'   # Renommé pour correspondre à un style cohérent
        })
        
        # S'assurer que 'revendeur_id' est un entier avant de créer commande_id_unique
        if 'revendeur_id' in df_commandes_csv.columns:
            df_commandes_csv['revendeur_id'] = pd.to_numeric(df_commandes_csv['revendeur_id'], errors='coerce').fillna(0).astype(int)
        
        # Table des Commandes
        # Crée un ID unique pour chaque commande basé sur le numéro de commande, la date et le revendeur
        df_commandes_csv['commande_id_unique'] = df_commandes_csv.groupby(['numero_commande', 'date_commande', 'revendeur_id']).ngroup() + 1
        
        # Sélection des colonnes pour la table Commandes
        df_commandes = df_commandes_csv[['commande_id_unique', 'numero_commande', 'date_commande', 'revendeur_id']].drop_duplicates()
        df_commandes = df_commandes.rename(columns={'commande_id_unique': 'commande_id'}) # Renommer pour la colonne cible
        df_commandes['date_commande'] = pd.to_datetime(df_commandes['date_commande']) # Assurer que la date est au bon format
        if not df_commandes.empty:
            load_to_mysql(df_commandes, 'Commandes', engine)

        # Table des Lignes de Commande
        # Sélection des colonnes pour la table LignesCommande
        df_lignes_commande = df_commandes_csv[['commande_id_unique', 'product_id', 'quantite', 'prix_unitaire_vente']]
        # Créer un ID de ligne unique
        df_lignes_commande['ligne_id'] = range(1, len(df_lignes_commande) + 1)
        df_lignes_commande = df_lignes_commande.rename(columns={'commande_id_unique': 'commande_id'})
        df_lignes_commande = df_lignes_commande[['ligne_id', 'commande_id', 'product_id', 'quantite', 'prix_unitaire_vente']] # Réordonner les colonnes
        
        # Assurez-vous que les colonnes numériques sont au bon type
        df_lignes_commande['quantite'] = pd.to_numeric(df_lignes_commande['quantite'], errors='coerce').fillna(0).astype(int)
        df_lignes_commande['prix_unitaire_vente'] = pd.to_numeric(df_lignes_commande['prix_unitaire_vente'], errors='coerce').fillna(0.0).astype(float)

        if not df_lignes_commande.empty:
            load_to_mysql(df_lignes_commande, 'LignesCommande', engine)

        logging.info("Processus ETL terminé avec succès.")

    except FileNotFoundError as e:
        logging.error(f"❌ Échec du processus ETL : Fichier non trouvé - {e}")
    except mysql.connector.Error as e:
        logging.error(f"❌ Échec du processus ETL (Erreur MySQL) : {e}")
    except Exception as e:
        logging.error(f"❌ Échec du processus ETL : {e}")
    finally:
        # --- Réactiver les vérifications de clés étrangères dans le bloc finally ---
        if engine:
            try:
                with engine.connect() as connection:
                    logging.info("Réactivation des vérifications de clés étrangères...")
                    connection.execute(text("SET FOREIGN_KEY_CHECKS = 1;")) # Utiliser text()
                    connection.commit()
                    logging.info("Vérifications de clés étrangères réactivées.")
            except Exception as e:
                logging.error(f"Erreur lors de la réactivation des clés étrangères : {e}")
            finally:
                engine.dispose() # S'assurer que les connexions sont fermées

if __name__ == "__main__":
    main()