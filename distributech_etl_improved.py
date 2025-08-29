import pandas as pd
import sqlite3
import logging
from sqlalchemy import create_engine, types, text
import os
import mysql.connector
from datetime import datetime
import subprocess
import numpy as np

# === CONFIGURATION ===
MYSQL_USER = 'appuser'
MYSQL_PASSWORD = 'example_password'
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3307'
MYSQL_DB = 'distributech_db'

SQLITE_DB_PATH = './data/base_stock.sqlite'
CSV_PATH = 'commande_revendeur_tech_express.csv'
EXPORT_DIR = './exports'
os.makedirs(EXPORT_DIR, exist_ok=True)

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# === FONCTION : Validation des données ===
def validate_dataframe(df, table_name, required_columns, pk_column=None):
    """Valide la structure et la cohérence des données d'un DataFrame"""
    logging.info(f"🔍 Validation des données pour '{table_name}'")
    
    # Vérifier les colonnes requises
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"❌ Colonnes manquantes dans '{table_name}': {missing_cols}")
    
    # Vérifier les valeurs nulles dans la clé primaire
    if pk_column and df[pk_column].isnull().any():
        raise ValueError(f"❌ Valeurs nulles détectées dans la clé primaire '{pk_column}' de '{table_name}'")
    
    # Vérifier les doublons dans la clé primaire
    if pk_column and df[pk_column].duplicated().any():
        duplicates_count = df[pk_column].duplicated().sum()
        logging.warning(f"⚠️  {duplicates_count} doublons détectés dans '{pk_column}' de '{table_name}'")
        df = df.drop_duplicates(subset=[pk_column], keep='first')
    
    # Nettoyer les valeurs nulles dans les colonnes texte
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna('').astype(str).str.strip()
    
    logging.info(f"✅ Validation terminée pour '{table_name}' - {len(df)} lignes valides")
    return df


# === FONCTION : Créer l'utilisateur MySQL avec droits ===
def creer_utilisateur_mysql():
    """Crée l'utilisateur 'appuser'@'localhost' et lui attribue les droits nécessaires"""
    logging.info("🔧 Vérification/création de l'utilisateur MySQL 'appuser'")

    config_admin = {
        "host": MYSQL_HOST,
        "port": int(MYSQL_PORT),
        "user": "root",
        "password": "example_password",
        "database": MYSQL_DB
    }

    try:
        with mysql.connector.connect(**config_admin) as conn:
            cursor = conn.cursor()

            # Créer l'utilisateur s'il n'existe pas
            try:
                cursor.execute("CREATE USER 'appuser'@'localhost' IDENTIFIED BY 'example_password';")
                logging.info("✅ Utilisateur 'appuser'@'localhost' créé")
            except mysql.connector.Error as e:
                if e.errno == 1396:
                    logging.info("ℹ️  L'utilisateur 'appuser'@'localhost' existe déjà")
                else:
                    raise e

            # Attribuer les droits nécessaires
            privileges = (
                "SELECT, INSERT, UPDATE, DELETE, LOCK TABLES, "
                "SHOW VIEW, EVENT, TRIGGER"
            )
            cursor.execute(f"GRANT {privileges} ON {MYSQL_DB}.* TO 'appuser'@'localhost';")
            cursor.execute("FLUSH PRIVILEGES;")
            logging.info(f"✅ Droits attribués à 'appuser'@'localhost' sur `{MYSQL_DB}`")

    except mysql.connector.Error as e:
        logging.error(f"❌ Impossible de configurer l'utilisateur MySQL : {e}")
        raise


# === FONCTION : Créer les tables si elles n'existent pas ===
def create_table_if_not_exists(engine, create_table_sql):
    with engine.connect() as connection:
        try:
            connection.execute(text(create_table_sql))
            connection.commit()
            logging.info("✅ Table créée ou existe déjà")
        except Exception as e:
            logging.error(f"❌ Erreur lors de la création de la table : {e}")
            raise


# === FONCTION : Extraire CSV ===
def extract_csv(path):
    """Extrait et valide les données du fichier CSV des commandes"""
    logging.info("📥 Extraction du fichier CSV...")
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Fichier CSV introuvable : {path}")
    
    try:
        df = pd.read_csv(path)
        logging.info(f"✅ {len(df)} lignes extraites du CSV")
        
        # Validation des colonnes essentielles du CSV
        required_csv_columns = ['numero_commande', 'commande_date', 'revendeur_id', 'product_id', 'quantity', 'unit_price']
        missing_cols = [col for col in required_csv_columns if col not in df.columns]
        if missing_cols:
            logging.warning(f"⚠️  Colonnes manquantes dans le CSV : {missing_cols}")
        
        return df
    except Exception as e:
        logging.error(f"❌ Erreur lors de la lecture du CSV : {e}")
        raise


# === FONCTION : Extraire SQLite ===
def extract_sqlite(db_path):
    """Extrait toutes les données de la base SQLite"""
    logging.info(f"🗄️  Connexion à la base SQLite : {db_path}")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"❌ Base SQLite introuvable : {db_path}")

    with sqlite3.connect(db_path) as conn:
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
        data = {}
        for table in tables['name']:
            df = pd.read_sql(f"SELECT * FROM {table}", conn)
            data[table] = df
            logging.info(f"✅ Table '{table}' : {len(df)} lignes")
    return data


# === FONCTION : Charger avec anti-doublons ===
def load_to_mysql_deduplicated(df, table_name, engine, pk_column, index_as_pk=False):
    """Charge les données dans MySQL en évitant les doublons"""
    logging.info(f"🔁 Chargement dans MySQL (anti-doublons) : '{table_name}'")
    
    if df.empty:
        logging.info(f"🟡 Aucune donnée à charger dans '{table_name}'")
        return
    
    with engine.connect() as conn:
        # Vérifier si la table existe et récupérer les IDs existants
        try:
            conn.execute(text(f"SELECT 1 FROM `{table_name}` LIMIT 1"))
            has_table = True
        except Exception:
            has_table = False

        if has_table and pk_column:
            try:
                existing = pd.read_sql(f"SELECT `{pk_column}` FROM `{table_name}`", conn)
                existing_ids = existing[pk_column].dropna().tolist()
                original_count = len(df)
                df = df[~df[pk_column].isin(existing_ids)]
                logging.info(f"➡️  {len(df)} nouvelles lignes après filtrage des doublons ({original_count - len(df)} doublons évités)")
            except Exception as e:
                logging.warning(f"⚠️  Impossible de lire les IDs existants dans '{table_name}' : {e}")

    # Définir les types SQL appropriés
    dtype_mapping = {}
    for col in df.columns:
        if df[col].dtype == 'object':
            max_length = df[col].astype(str).str.len().max()
            length = min(max(max_length, 50), 500)  # Entre 50 et 500 caractères
            dtype_mapping[col] = types.String(length)
        elif df[col].dtype in ['int64', 'int32']:
            dtype_mapping[col] = types.BigInteger()
        elif df[col].dtype in ['float64', 'float32']:
            dtype_mapping[col] = types.Float()
        elif 'datetime' in str(df[col].dtype):
            dtype_mapping[col] = types.DateTime()

    if not df.empty:
        try:
            df.to_sql(
                table_name,
                con=engine,
                if_exists='append',
                index=index_as_pk,
                dtype=dtype_mapping
            )
            logging.info(f"✅ {len(df)} lignes insérées dans '{table_name}'")
        except Exception as e:
            logging.error(f"❌ Échec du chargement dans '{table_name}' : {e}")
            raise
    else:
        logging.info(f"🟡 Aucune nouvelle ligne à insérer dans '{table_name}'")


# === FONCTION : Créer les mouvements de stock ===
def create_mouvements_stock(engine, commandes_df, productions_df=None):
    """Crée les mouvements de stock basés sur les commandes et productions"""
    logging.info("📦 Création des mouvements de stock...")
    
    mouvements = []
    
    # Mouvements de sortie (commandes)
    if not commandes_df.empty:
        for _, row in commandes_df.iterrows():
            mouvement = {
                'produit_id': row['produit_id'],
                'type_mouvement': 'SORTIE',
                'quantite': -abs(row['quantite']),  # Négatif pour les sorties
                'date_mouvement': row['date_commande'],
                'reference': f"CMD-{row['numero_commande']}",
                'commande_id': row['commande_id']
            }
            mouvements.append(mouvement)
    
    # Mouvements d'entrée (productions/réapprovisionnements)
    if productions_df is not None and not productions_df.empty:
        for _, row in productions_df.iterrows():
            mouvement = {
                'produit_id': row['product_id'],
                'type_mouvement': 'ENTREE',
                'quantite': abs(row['quantite_produite']),  # Positif pour les entrées
                'date_mouvement': row['date'],
                'reference': f"PROD-{row['production_id']}",
                'commande_id': None
            }
            mouvements.append(mouvement)
    
    if mouvements:
        df_mouvements = pd.DataFrame(mouvements)
        df_mouvements['mouvement_id'] = range(1, len(df_mouvements) + 1)
        df_mouvements['date_mouvement'] = pd.to_datetime(df_mouvements['date_mouvement'])
        
        # Valider les données
        df_mouvements = validate_dataframe(
            df_mouvements, 
            'MouvementsStock', 
            ['mouvement_id', 'produit_id', 'type_mouvement', 'quantite', 'date_mouvement'],
            'mouvement_id'
        )
        
        load_to_mysql_deduplicated(df_mouvements, 'MouvementsStock', engine, 'mouvement_id')
        logging.info(f"✅ {len(df_mouvements)} mouvements de stock créés")


# === FONCTION : Export SQL complet ===
def export_sql_complet():
    """Exporte la base complète en SQL"""
    logging.info("📦 Démarrage de l'export SQL complet...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{EXPORT_DIR}/distributech_full_backup_{timestamp}.sql"

    try:
        cmd = [
            "mysqldump",
            f"--host={MYSQL_HOST}",
            f"--port={MYSQL_PORT}",
            "--single-transaction",
            "--routines",
            "--triggers",
            f"--user={MYSQL_USER}",
            f"--password={MYSQL_PASSWORD}",
            MYSQL_DB
        ]
        with open(output_file, "w", encoding="utf-8") as f:
            subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True, check=True)
        logging.info(f"✅ Export SQL terminé : {output_file}")
        return output_file
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Échec de mysqldump : {e.stderr}")
        return None
    except Exception as e:
        logging.error(f"❌ Erreur inattendue lors de l'export SQL : {e}")
        return None


# === FONCTION : Export état des stocks amélioré ===
def export_etat_stocks(engine):
    """Génère un rapport détaillé de l'état des stocks"""
    logging.info("📊 Génération de l'état des stocks par produit...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{EXPORT_DIR}/etat_des_stocks_{timestamp}.csv"

    query = """
    SELECT 
        p.produit_id,
        p.nom_produit,
        p.prix_unitaire,
        COALESCE(entrees.total_entrees, 0) AS total_entrees,
        COALESCE(sorties.total_sorties, 0) AS total_sorties,
        COALESCE(entrees.total_entrees, 0) + COALESCE(sorties.total_sorties, 0) AS stock_actuel,
        CASE 
            WHEN COALESCE(entrees.total_entrees, 0) + COALESCE(sorties.total_sorties, 0) <= 0 
            THEN 'RUPTURE'
            WHEN COALESCE(entrees.total_entrees, 0) + COALESCE(sorties.total_sorties, 0) <= 10 
            THEN 'FAIBLE'
            ELSE 'OK'
        END AS statut_stock,
        COALESCE(derniere_commande.derniere_date, 'Jamais') AS derniere_commande,
        COALESCE(nb_commandes.total_commandes, 0) AS nombre_commandes_total
    FROM Produits p
    LEFT JOIN (
        SELECT 
            produit_id, 
            SUM(quantite) as total_entrees
        FROM MouvementsStock 
        WHERE type_mouvement = 'ENTREE'
        GROUP BY produit_id
    ) entrees ON p.produit_id = entrees.produit_id
    LEFT JOIN (
        SELECT 
            produit_id, 
            SUM(quantite) as total_sorties
        FROM MouvementsStock 
        WHERE type_mouvement = 'SORTIE'
        GROUP BY produit_id
    ) sorties ON p.produit_id = sorties.produit_id
    LEFT JOIN (
        SELECT 
            ms.produit_id,
            MAX(ms.date_mouvement) as derniere_date
        FROM MouvementsStock ms
        WHERE ms.type_mouvement = 'SORTIE'
        GROUP BY ms.produit_id
    ) derniere_commande ON p.produit_id = derniere_commande.produit_id
    LEFT JOIN (
        SELECT 
            ms.produit_id,
            COUNT(DISTINCT ms.commande_id) as total_commandes
        FROM MouvementsStock ms
        WHERE ms.type_mouvement = 'SORTIE' AND ms.commande_id IS NOT NULL
        GROUP BY ms.produit_id
    ) nb_commandes ON p.produit_id = nb_commandes.produit_id
    ORDER BY stock_actuel ASC, p.produit_id;
    """

    try:
        df_stock = pd.read_sql(query, engine)
        df_stock.to_csv(output_file, index=False, encoding='utf-8')
        logging.info(f"✅ État des stocks exporté : {output_file}")
        logging.info(f"📈 {len(df_stock)} produits dans le rapport")
        
        # Statistiques rapides
        ruptures = len(df_stock[df_stock['statut_stock'] == 'RUPTURE'])
        faibles = len(df_stock[df_stock['statut_stock'] == 'FAIBLE'])
        logging.info(f"📊 Statistiques : {ruptures} ruptures, {faibles} stocks faibles")
        
        return output_file
    except Exception as e:
        logging.error(f"❌ Échec de génération de l'état des stocks : {e}")
        return None


# === MAIN ===
def main():
    logging.info("🚀 Démarrage du script ETL Distributech")

    try:
        # --- 1. Créer l'utilisateur MySQL ---
        creer_utilisateur_mysql()

        # --- 2. Créer l'engine SQLAlchemy ---
        mysql_url = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        engine = create_engine(mysql_url, echo=False)

        # --- 3. Extraire les données ---
        df_csv = extract_csv(CSV_PATH)
        sqlite_data = extract_sqlite(SQLITE_DB_PATH)

        # --- 4. Créer les tables ---
        logging.info("🏗️  Création des tables...")
        
        create_table_if_not_exists(engine, """
        CREATE TABLE IF NOT EXISTS Regions (
            region_id INT PRIMARY KEY,
            nom_region VARCHAR(255) NOT NULL
        )""")

        create_table_if_not_exists(engine, """
        CREATE TABLE IF NOT EXISTS Revendeurs (
            revendeur_id INT PRIMARY KEY,
            nom_revendeur VARCHAR(255) NOT NULL,
            region_id INT,
            email_contact VARCHAR(255),
            FOREIGN KEY (region_id) REFERENCES Regions(region_id)
        )""")

        create_table_if_not_exists(engine, """
        CREATE TABLE IF NOT EXISTS Produits (
            produit_id INT PRIMARY KEY,
            nom_produit VARCHAR(255) NOT NULL,
            prix_unitaire DECIMAL(10,2)
        )""")

        create_table_if_not_exists(engine, """
        CREATE TABLE IF NOT EXISTS Productions (
            production_id INT PRIMARY KEY,
            product_id INT NOT NULL,
            quantite_produite INT NOT NULL,
            date DATE NOT NULL,
            FOREIGN KEY (product_id) REFERENCES Produits(produit_id)
        )""")

        create_table_if_not_exists(engine, """
        CREATE TABLE IF NOT EXISTS Commandes (
            commande_id INT PRIMARY KEY,
            numero_commande VARCHAR(255) NOT NULL,
            date_commande DATETIME NOT NULL,
            revendeur_id INT NOT NULL,
            FOREIGN KEY (revendeur_id) REFERENCES Revendeurs(revendeur_id)
        )""")

        create_table_if_not_exists(engine, """
        CREATE TABLE IF NOT EXISTS LignesCommande (
            ligne_id INT PRIMARY KEY,
            commande_id INT NOT NULL,
            produit_id INT NOT NULL,
            quantite INT NOT NULL,
            prix_unitaire_vente DECIMAL(10,2),
            FOREIGN KEY (commande_id) REFERENCES Commandes(commande_id),
            FOREIGN KEY (produit_id) REFERENCES Produits(produit_id)
        )""")

        create_table_if_not_exists(engine, """
        CREATE TABLE IF NOT EXISTS MouvementsStock (
            mouvement_id INT PRIMARY KEY,
            produit_id INT NOT NULL,
            type_mouvement ENUM('ENTREE', 'SORTIE') NOT NULL,
            quantite INT NOT NULL,
            date_mouvement DATETIME NOT NULL,
            reference VARCHAR(255),
            commande_id INT,
            FOREIGN KEY (produit_id) REFERENCES Produits(produit_id),
            FOREIGN KEY (commande_id) REFERENCES Commandes(commande_id)
        )""")

        # --- 5. Charger les données SQLite ---
        logging.info("📤 Chargement des données SQLite...")
        
        if 'region' in sqlite_data:
            df = sqlite_data['region'].rename(columns={'region_name': 'nom_region'})
            df = validate_dataframe(df, 'Regions', ['region_id', 'nom_region'], 'region_id')
            load_to_mysql_deduplicated(df, 'Regions', engine, pk_column='region_id')

        if 'revendeur' in sqlite_data:
            df = sqlite_data['revendeur'].rename(columns={'revendeur_name': 'nom_revendeur'})
            # Générer des emails plus réalistes
            df['email_contact'] = df.apply(lambda x: 
                f"{x['nom_revendeur'].lower().replace(' ', '.').replace('é', 'e').replace('è', 'e')}@{x['nom_revendeur'].lower().replace(' ', '')}.com", 
                axis=1)
            df = validate_dataframe(df, 'Revendeurs', ['revendeur_id', 'nom_revendeur'], 'revendeur_id')
            load_to_mysql_deduplicated(df, 'Revendeurs', engine, pk_column='revendeur_id')

        if 'produit' in sqlite_data:
            df = sqlite_data['produit'].rename(columns={
                'product_name': 'nom_produit',
                'cout_unitaire': 'prix_unitaire',
                'product_id': 'produit_id'
            })
            df = validate_dataframe(df, 'Produits', ['produit_id', 'nom_produit'], 'produit_id')
            load_to_mysql_deduplicated(df, 'Produits', engine, pk_column='produit_id')

        productions_df = None
        if 'production' in sqlite_data:
            productions_df = sqlite_data['production'].rename(columns={
                'quantity': 'quantite_produite',
                'date_production': 'date'
            }).reset_index()
            productions_df['production_id'] = productions_df.index + 1
            productions_df['date'] = pd.to_datetime(productions_df['date'])
            productions_df = validate_dataframe(productions_df, 'Productions', 
                                              ['production_id', 'product_id', 'quantite_produite', 'date'], 
                                              'production_id')
            load_to_mysql_deduplicated(productions_df, 'Productions', engine, pk_column='production_id')

        # --- 6. Traiter les commandes CSV ---
        logging.info("📦 Traitement des commandes CSV...")
        
        df_csv = df_csv.rename(columns={
            'commande_date': 'date_commande',
            'quantity': 'quantite',
            'unit_price': 'prix_unitaire_vente'
        })

        # Générer un ID unique par commande basé sur numero_commande et date
        df_csv['date_commande'] = pd.to_datetime(df_csv['date_commande'])
        df_csv['commande_key'] = df_csv['numero_commande'] + '_' + df_csv['date_commande'].dt.strftime('%Y%m%d')
        commande_mapping = {key: idx + 1 for idx, key in enumerate(df_csv['commande_key'].unique())}
        df_csv['commande_id'] = df_csv['commande_key'].map(commande_mapping)

        # Charger les commandes
        commandes = df_csv[['commande_id', 'numero_commande', 'date_commande', 'revendeur_id']].drop_duplicates()
        commandes = validate_dataframe(commandes, 'Commandes', 
                                     ['commande_id', 'numero_commande', 'date_commande', 'revendeur_id'], 
                                     'commande_id')
        load_to_mysql_deduplicated(commandes, 'Commandes', engine, pk_column='commande_id')

        # Charger les lignes de commande
        lignes = df_csv[['commande_id', 'product_id', 'quantite', 'prix_unitaire_vente']].copy()
        lignes['ligne_id'] = range(1, len(lignes) + 1)
        lignes = lignes.rename(columns={'product_id': 'produit_id'})
        lignes = validate_dataframe(lignes, 'LignesCommande', 
                                  ['ligne_id', 'commande_id', 'produit_id', 'quantite'], 
                                  'ligne_id')
        load_to_mysql_deduplicated(lignes, 'LignesCommande', engine, pk_column='ligne_id')

        # --- 7. Créer les mouvements de stock ---
        commandes_mouvements = df_csv[['commande_id', 'numero_commande', 'date_commande', 'product_id', 'quantite']].rename(columns={'product_id': 'produit_id'})
        create_mouvements_stock(engine, commandes_mouvements, productions_df)

        # --- 8. Générer les exports ---
        logging.info("📤 Génération des exports finaux...")
        sql_file = export_sql_complet()
        stock_file = export_etat_stocks(engine)

        # --- 9. Résumé final ---
        logging.info("=" * 50)
        logging.info("✅ SCRIPT ETL TERMINÉ AVEC SUCCÈS")
        logging.info("=" * 50)
        logging.info(f"📁 Fichiers générés :")
        if sql_file:
            logging.info(f"   • Export SQL : {sql_file}")
        if stock_file:
            logging.info(f"   • État stocks : {stock_file}")
        
        # Statistiques finales
        with engine.connect() as conn:
            stats = {
                'regions': pd.read_sql("SELECT COUNT(*) as count FROM Regions", conn).iloc[0]['count'],
                'revendeurs': pd.read_sql("SELECT COUNT(*) as count FROM Revendeurs", conn).iloc[0]['count'],
                'produits': pd.read_sql("SELECT COUNT(*) as count FROM Produits", conn).iloc[0]['count'],
                'commandes': pd.read_sql("SELECT COUNT(*) as count FROM Commandes", conn).iloc[0]['count'],
                'mouvements': pd.read_sql("SELECT COUNT(*) as count FROM MouvementsStock", conn).iloc[0]['count']
            }
        
        logging.info(f"📊 Données chargées :")
        for table, count in stats.items():
            logging.info(f"   • {table.capitalize()} : {count} enregistrements")

    except Exception as e:
        logging.error(f"❌ ERREUR CRITIQUE : {e}")
        raise


if __name__ == "__main__":
    main()