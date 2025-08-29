import pandas as pd
import sqlite3
import logging
from sqlalchemy import create_engine, types, text
import os
import mysql.connector

# --- Configuration commune ---
MYSQL_USER = 'appuser'
MYSQL_PASSWORD = 'example_password'
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3307'
MYSQL_DB = 'distributech_db'

SQLITE_DB_PATH = './data/base_stock.sqlite'
CSV_PATH = 'commande_revendeur_tech_express.csv'
STOCK_CSV_PATH = 'etat_des_stocks.csv' # Nouveau chemin pour le CSV de stock

# --- Configuration du logger ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Fonctions d'Extraction (existantes) ---
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

# --- Fonction de Chargement (existante) ---
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
            if index_as_pk and df.index.name == col:
                dtype_mapping[col] = types.Integer()
            else:
                dtype_mapping[col] = types.BigInteger()
        elif df[col].dtype == 'float64':
            dtype_mapping[col] = types.Float()
        elif df[col].dtype == 'datetime64[ns]':
            dtype_mapping[col] = types.DateTime()
        else:
            dtype_mapping[col] = None

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

# --- Nouvelle fonction pour générer le rapport de stock (intégrée) ---
def generate_stock_report_csv(mysql_config, output_csv_path):
    """
    Se connecte à la base de données MySQL, calcule l'état des stocks
    à partir des productions et exporte le résultat dans un CSV.
    """
    logging.info("🚀 Démarrage de la génération du rapport de stock CSV.")
    conn = None
    try:
        conn = mysql.connector.connect(
            host=mysql_config['host'],
            port=mysql_config['port'],
            database=mysql_config['database'],
            user=mysql_config['user'],
            password=mysql_config['password']
        )

        if conn.is_connected():
            logging.info("Connexion MySQL pour le rapport de stock établie.")
            
            query = """
            SELECT
                p.produit_id,
                p.nom_produit,
                SUM(prod.quantite_produite) AS total_stock_produit
            FROM
                Produits p
            JOIN
                Productions prod ON p.produit_id = prod.product_id
            GROUP BY
                p.produit_id, p.nom_produit
            ORDER BY
                p.produit_id;
            """
            
            df_stock = pd.read_sql(query, conn)
            
            df_stock.to_csv(output_csv_path, index=False)
            logging.info(f"✅ Rapport de stock exporté avec succès vers '{output_csv_path}'")
            logging.info(f"Aperçu du contenu du fichier '{output_csv_path}':\n{df_stock.to_string(index=False)}")

        else:
            logging.error("❌ Échec de la connexion à la base de données MySQL pour le rapport de stock.")

    except mysql.connector.Error as e:
        logging.error(f"❌ Erreur MySQL lors de la génération du rapport de stock : {e}")
        raise # Rélance l'exception pour que le bloc main puisse la gérer
    except Exception as e:
        logging.error(f"❌ Une erreur inattendue est survenue lors de la génération du rapport de stock : {e}")
        raise # Rélance l'exception
    finally:
        if conn and conn.is_connected():
            conn.close()
            logging.info("Connexion MySQL pour le rapport de stock fermée.")

# --- Fonction principale unifiée ---
def main():
    logging.info("🚀 Lancement du script ETL unifié")
    engine = None
    try:
        mysql_url = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        engine = create_engine(mysql_url)
        
        # Préparer la configuration MySQL pour la fonction de rapport de stock
        mysql_config_for_report = {
            'host': MYSQL_HOST,
            'port': int(MYSQL_PORT), # Assurez-vous que le port est un entier
            'database': MYSQL_DB,
            'user': MYSQL_USER,
            'password': MYSQL_PASSWORD
        }

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
            
            if df_regions.index.name == 'region_id':
                 df_regions = df_regions.reset_index()
            
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
            df_revendeurs['region_id'] = df_revendeurs['region_id'].astype(int)
            load_to_mysql(df_revendeurs, 'Revendeurs', engine, index_as_pk=False, if_exists_mode='append')

        # Produits : Créer explicitement la table pour assurer le type PRIMARY KEY
        df_produits = sqlite_data.get('produit', pd.DataFrame()).rename(columns={'product_name': 'nom_produit', 'cout_unitaire': 'prix_unitaire'})
        if not df_produits.empty:
            df_produits = df_produits.rename(columns={'product_id': 'produit_id'})
            
            logging.info("Attempting to explicitly create 'Produits' table...")
            with engine.connect() as connection:
                try:
                    connection.execute(text("DROP TABLE IF EXISTS `Produits`;"))
                    logging.info("Dropped existing 'Produits' table if it existed.")
                    connection.execute(text("""
                        CREATE TABLE `Produits` (
                            produit_id INT PRIMARY KEY,
                            nom_produit VARCHAR(255),
                            prix_unitaire FLOAT
                        );
                    """))
                    connection.commit()
                    logging.info("✨ Table 'Produits' créée avec 'produit_id' comme clé primaire INT.")
                except Exception as e:
                    logging.error(f"❌ Erreur lors de la création explicite de la table 'Produits': {e}")
                    raise
            
            if df_produits.index.name == 'produit_id':
                df_produits = df_produits.reset_index()

            df_produits['produit_id'] = df_produits['produit_id'].astype(int)
            load_to_mysql(df_produits, 'Produits', engine, index_as_pk=False, if_exists_mode='append')
            
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
        
        if 'revendeur_id' in df_commandes_csv.columns:
            df_commandes_csv['revendeur_id'] = pd.to_numeric(df_commandes_csv['revendeur_id'], errors='coerce').fillna(0).astype(int)
        
        df_commandes_csv['commande_id'] = df_commandes_csv.groupby(['numero_commande', 'date_commande', 'revendeur_id']).ngroup() + 1
        
        df_commandes = df_commandes_csv[['commande_id', 'numero_commande', 'date_commande', 'revendeur_id']].drop_duplicates()
        df_commandes['date_commande'] = pd.to_datetime(df_commandes['date_commande'])
        
        if not df_commandes.empty:
            logging.info("Attempting to explicitly create 'Commandes' table...")
            with engine.connect() as connection:
                try:
                    connection.execute(text("DROP TABLE IF EXISTS `Commandes`;"))
                    logging.info("Dropped existing 'Commandes' table if it existed.")
                    connection.execute(text("""
                        CREATE TABLE `Commandes` (
                            commande_id INT PRIMARY KEY,
                            numero_commande VARCHAR(255),
                            date_commande DATETIME,
                            revendeur_id INT,
                            FOREIGN KEY (revendeur_id) REFERENCES Revendeurs(revendeur_id)
                        );
                    """))
                    connection.commit()
                    logging.info("✨ Table 'Commandes' créée avec 'commande_id' comme clé primaire INT et FK.")
                except Exception as e:
                    logging.error(f"❌ Erreur lors de la création explicite de la table 'Commandes': {e}")
                    raise

            if df_commandes.index.name == 'commande_id':
                 df_commandes = df_commandes.reset_index()
            
            df_commandes['commande_id'] = df_commandes['commande_id'].astype(int)
            df_commandes['revendeur_id'] = df_commandes['revendeur_id'].astype(int)
            
            load_to_mysql(df_commandes, 'Commandes', engine, index_as_pk=False, if_exists_mode='append')


        # Lignes de Commande (à partir du CSV)
        df_lignes_commande = df_commandes_csv[['commande_id', 'product_id', 'quantite', 'prix_unitaire_vente']]
        df_lignes_commande['ligne_id'] = range(1, len(df_lignes_commande) + 1)
        
        df_lignes_commande = df_lignes_commande.rename(columns={'product_id': 'produit_id'})

        df_lignes_commande = df_lignes_commande[['ligne_id', 'commande_id', 'produit_id', 'quantite', 'prix_unitaire_vente']]
        
        df_lignes_commande['quantite'] = pd.to_numeric(df_lignes_commande['quantite'], errors='coerce').fillna(0).astype(int)
        df_lignes_commande['prix_unitaire_vente'] = pd.to_numeric(df_lignes_commande['prix_unitaire_vente'], errors='coerce').fillna(0.0).astype(float)

        if not df_lignes_commande.empty:
            logging.info("Attempting to explicitly create 'LignesCommande' table...")
            with engine.connect() as connection:
                try:
                    connection.execute(text("DROP TABLE IF EXISTS `LignesCommande`;"))
                    logging.info("Dropped existing 'LignesCommande' table if it existed.")
                    connection.execute(text("""
                        CREATE TABLE `LignesCommande` (
                            ligne_id INT PRIMARY KEY,
                            commande_id INT,
                            produit_id INT,
                            quantite INT,
                            prix_unitaire_vente FLOAT,
                            FOREIGN KEY (commande_id) REFERENCES Commandes(commande_id),
                            FOREIGN KEY (produit_id) REFERENCES Produits(produit_id)
                        );
                    """))
                    connection.commit()
                    logging.info("✨ Table 'LignesCommande' créée avec clés primaires et étrangères.")
                except Exception as e:
                    logging.error(f"❌ Erreur lors de la création explicite de la table 'LignesCommande': {e}")
                    raise

            df_lignes_commande = df_lignes_commande.set_index('ligne_id')
            load_to_mysql(df_lignes_commande, 'LignesCommande', engine, index_as_pk=True, if_exists_mode='append')

        logging.info("✅ ETL terminé avec succès")

        # --- Appel de la fonction de génération du rapport de stock ---
        logging.info("Démarrage de la génération du rapport de stock après ETL.")
        generate_stock_report_csv(mysql_config_for_report, STOCK_CSV_PATH)
        logging.info("✅ Script unifié terminé avec succès.")

    except FileNotFoundError as e:
        logging.error(f"❌ Échec du processus ETL unifié : Fichier non trouvé - {e}")
    except mysql.connector.Error as e:
        logging.error(f"❌ Échec du processus ETL unifié (Erreur MySQL) : {e}")
    except Exception as e:
        logging.error(f"❌ Échec du processus ETL unifié : {e}")
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