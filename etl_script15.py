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

# CHEMINS : Assurez-vous que ces chemins correspondent à l'emplacement de vos fichiers
SQLITE_DB_PATH = './data/base_stock.sqlite'
CSV_PATH = 'commande_revendeur_tech_express.csv'

# --- Configuration du logger ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_csv(path):
    """Extrait les données d'un fichier CSV. Ajout d'une vérification d'existence."""
    logging.info("📥 Extraction du CSV...")
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV introuvable : {path}")
    df = pd.read_csv(path)
    logging.info(f"{len(df)} lignes extraites depuis le CSV")
    return df

def extract_sqlite(db_path):
    """Extrait les données de toutes les tables d'une base de données SQLite. Ajout d'une vérification d'existence."""
    logging.info(f"📥 Connexion SQLite : {db_path}...")
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

def load_to_mysql(df, table_name, engine, index_as_pk=False, if_exists_mode='replace'):
    """
    Charge un DataFrame dans une table MySQL spécifique.
    index_as_pk=True indique que l'index du DataFrame doit être traité comme clé primaire.
    if_exists_mode='replace' (par défaut) ou 'append'.
    """
    logging.info(f"📤 Chargement dans MySQL : '{table_name}' avec if_exists='{if_exists_mode}'...")
    
    dtype_mapping = {}
    for col in df.columns:
        if df[col].dtype == 'object':
            dtype_mapping[col] = types.String(length=255) 
        elif df[col].dtype == 'int64':
            # Utiliser INTEGER pour les clés primaires ou étrangères qui sont généralement de petits entiers
            # Si la colonne est l'index du DataFrame ET index_as_pk est True (ce qui signifie qu'elle sera la PK)
            if index_as_pk and df.index.name == col:
                dtype_mapping[col] = types.Integer() # Forcer INTEGER pour la clé primaire
            else:
                dtype_mapping[col] = types.BigInteger() # Par défaut pour les autres colonnes int64
        elif df[col].dtype == 'float64':
            dtype_mapping[col] = types.Float()
        elif df[col].dtype == 'datetime64[ns]':
            dtype_mapping[col] = types.DateTime()
        else:
            dtype_mapping[col] = None

    # Cette partie gère si l'index lui-même est censé être mappé (par exemple, si index_as_pk=True et l'index n'est pas une colonne)
    if index_as_pk and df.index.name is not None and df.index.name not in df.columns:
        if df.index.dtype == 'int64':
            dtype_mapping[df.index.name] = types.Integer()
        elif df.index.dtype == 'object':
            dtype_mapping[df.index.name] = types.String(length=255)
            
    try:
        df.to_sql(table_name, con=engine, if_exists=if_exists_mode, index=index_as_pk, dtype=dtype_mapping)
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
        
        # Connexion et désactivation des vérifications de clés étrangères
        with engine.connect() as connection:
            logging.info("Connexion à la base de données MySQL établie.")
            connection.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            connection.commit()
            logging.info("🔓 Clés étrangères désactivées.")

        # Extraction des données
        df_csv = extract_csv(CSV_PATH)
        sqlite_data = extract_sqlite(SQLITE_DB_PATH)

        # --- Transformation et Chargement des données ---

        # Régions : Créer explicitement la table pour assurer le type PRIMARY KEY et INT
        df_regions = sqlite_data.get('region', pd.DataFrame()).rename(columns={'region_name': 'nom_region'})
        if not df_regions.empty:
            logging.info("Attempting to explicitly create 'Regions' table...")
            with engine.connect() as connection:
                try:
                    connection.execute(text("DROP TABLE IF EXISTS `Regions`;"))
                    logging.info("Dropped existing 'Regions' table if it existed.")
                    connection.execute(text("""
                        CREATE TABLE `Regions` (
                            region_id INT PRIMARY KEY,
                            nom_region VARCHAR(255)
                        );
                    """))
                    connection.commit()
                    logging.info("✨ Table 'Regions' créée avec 'region_id' comme clé primaire INT.")
                except Exception as e:
                    logging.error(f"❌ Erreur lors de la création explicite de la table 'Regions': {e}")
                    raise
            
            # S'assurer que 'region_id' est une colonne pour l'ajout (si c'était un index, le réinitialiser)
            if df_regions.index.name == 'region_id':
                 df_regions = df_regions.reset_index()
            
            # S'assurer que le type est correct avant le chargement
            df_regions['region_id'] = df_regions['region_id'].astype(int)
            load_to_mysql(df_regions, 'Regions', engine, index_as_pk=False, if_exists_mode='append')


        # Revendeurs : Créer explicitement la table pour assurer le type PRIMARY KEY et FOREIGN KEY
        df_revendeurs = sqlite_data.get('revendeur', pd.DataFrame()).rename(columns={'revendeur_name': 'nom_revendeur'})
        df_revendeurs['email_contact'] = df_revendeurs['nom_revendeur'].apply(lambda x: f"{x.lower().replace(' ', '')}@exemple.com")
        if not df_revendeurs.empty:
            logging.info("Attempting to explicitly create 'Revendeurs' table...")
            with engine.connect() as connection:
                try:
                    connection.execute(text("DROP TABLE IF EXISTS `Revendeurs`;"))
                    logging.info("Dropped existing 'Revendeurs' table if it existed.")
                    connection.execute(text("""
                        CREATE TABLE `Revendeurs` (
                            revendeur_id INT PRIMARY KEY,
                            nom_revendeur VARCHAR(255),
                            region_id INT,
                            email_contact VARCHAR(255),
                            FOREIGN KEY (region_id) REFERENCES Regions(region_id)
                        );
                    """))
                    connection.commit()
                    logging.info("✨ Table 'Revendeurs' créée avec 'revendeur_id' comme clé primaire INT et FK.")
                except Exception as e:
                    logging.error(f"❌ Erreur lors de la création explicite de la table 'Revendeurs': {e}")
                    raise

            if df_revendeurs.index.name == 'revendeur_id':
                 df_revendeurs = df_revendeurs.reset_index()
            df_revendeurs['revendeur_id'] = df_revendeurs['revendeur_id'].astype(int)
            df_revendeurs['region_id'] = df_revendeurs['region_id'].astype(int) # S'assurer que la colonne FK est aussi INT
            load_to_mysql(df_revendeurs, 'Revendeurs', engine, index_as_pk=False, if_exists_mode='append')

        # Produits : Peut toujours utiliser index_as_pk=True car ce n'est pas la racine du problème de FK
        df_produits = sqlite_data.get('produit', pd.DataFrame()).rename(columns={'product_name': 'nom_produit', 'cout_unitaire': 'prix_unitaire'})
        if not df_produits.empty:
            df_produits = df_produits.set_index('product_id')
            load_to_mysql(df_produits, 'Produits', engine, index_as_pk=True)
            
        # Production : Peut toujours utiliser index_as_pk=True
        df_production = sqlite_data.get('production', pd.DataFrame()).rename(columns={'quantity': 'quantite_produite', 'date_production': 'date'})
        if not df_production.empty:
             df_production = df_production.set_index('production_id')
             load_to_mysql(df_production, 'Productions', engine, index_as_pk=True)

        # Commandes (à partir du CSV)
        df_commandes_csv = df_csv.rename(columns={
            'numero_commande': 'numero_commande',
            'commande_date': 'date_commande',
            'quantity': 'quantite',
            'unit_price': 'prix_unitaire_vente'
        })
        
        # S'assurer que revendeur_id du CSV est un int
        if 'revendeur_id' in df_commandes_csv.columns:
            df_commandes_csv['revendeur_id'] = pd.to_numeric(df_commandes_csv['revendeur_id'], errors='coerce').fillna(0).astype(int)
        
        # Créer un ID unique pour chaque commande
        df_commandes_csv['commande_id'] = df_commandes_csv.groupby(['numero_commande', 'date_commande', 'revendeur_id']).ngroup() + 1
        
        df_commandes = df_commandes_csv[['commande_id', 'numero_commande', 'date_commande', 'revendeur_id']].drop_duplicates()
        df_commandes['date_commande'] = pd.to_datetime(df_commandes['date_commande'])
        if not df_commandes.empty:
            df_commandes = df_commandes.set_index('commande_id')
            load_to_mysql(df_commandes, 'Commandes', engine, index_as_pk=True)

        # Lignes de Commande (à partir du CSV)
        df_lignes_commande = df_commandes_csv[['commande_id', 'product_id', 'quantite', 'prix_unitaire_vente']]
        df_lignes_commande['ligne_id'] = range(1, len(df_lignes_commande) + 1)
        
        df_lignes_commande = df_lignes_commande[['ligne_id', 'commande_id', 'product_id', 'quantite', 'prix_unitaire_vente']]
        
        df_lignes_commande['quantite'] = pd.to_numeric(df_lignes_commande['quantite'], errors='coerce').fillna(0).astype(int)
        df_lignes_commande['prix_unitaire_vente'] = pd.to_numeric(df_lignes_commande['prix_unitaire_vente'], errors='coerce').fillna(0.0).astype(float)

        if not df_lignes_commande.empty:
            df_lignes_commande = df_lignes_commande.set_index('ligne_id')
            load_to_mysql(df_lignes_commande, 'LignesCommande', engine, index_as_pk=True)

        logging.info("✅ ETL terminé avec succès")

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
                    logging.info("🔒 Clés étrangères réactivées.")
                    connection.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
                    connection.commit()
            except Exception as e:
                logging.error(f"Erreur lors de la réactivation des clés étrangères : {e}")
            finally:
                if engine:
                    engine.dispose()

if __name__ == "__main__":
    main()