import os
import pandas as pd
import sqlite3
import mysql.connector
import logging
import subprocess
from datetime import datetime

# === CONFIGURATION ===
MYSQL_USER = 'appuser'
MYSQL_PASSWORD = 'example_password'
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3307'
MYSQL_DB = 'distributech_db'

SQLITE_DB_PATH = './data/base_stock.sqlite'
CSV_PATH = 'commande_revendeur_tech_express.csv'
EXPORT_DIR = "./exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

MYSQL_CONFIG = {
    "host": MYSQL_HOST,
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "database": MYSQL_DB,
    "port": MYSQL_PORT
}

# === MAPPINGS DES COLONNES ===
COLUMN_MAPPINGS = {
    "Regions": {
        "source_columns": ["region_id", "region_name"],
        "target_columns": ["region_id", "nom_region"]
    },
    "Revendeurs": {
        "source_columns": ["revendeur_id", "revendeur_name", "region_id"],
        "target_columns": ["revendeur_id", "nom_revendeur", "region_id"]
    },
    "Produits": {
        "source_columns": ["product_id", "product_name", "cout_unitaire"],
        "target_columns": ["produit_id", "nom_produit", "prix_unitaire"]
    },
    "Productions": {
        "source_columns": ["production_id", "product_id", "quantity", "date_production"],
        "target_columns": ["production_id", "product_id", "quantite_produite", "date"]
    },
    "Commandes": {
        "source_columns": ["numero_commande", "commande_date", "revendeur_id"],
        "target_columns": ["numero_commande", "date_commande", "revendeur_id"]
    },
    "LignesCommande": {
        "source_columns": ["commande_id", "product_id", "quantity", "unit_price"],
        "target_columns": ["commande_id", "produit_id", "quantite", "prix_unitaire_vente"]
    }
}

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === FONCTIONS ===

def mapper_colonnes(df, table_name):
    """Mappe les colonnes selon le dictionnaire de mapping"""
    if table_name not in COLUMN_MAPPINGS:
        logging.warning(f"Pas de mapping défini pour {table_name}")
        return df
    
    mapping = COLUMN_MAPPINGS[table_name]
    source_cols = mapping["source_columns"]
    target_cols = mapping["target_columns"]
    
    # Vérifier que les colonnes sources existent
    colonnes_disponibles = [col for col in source_cols if col in df.columns]
    if not colonnes_disponibles:
        logging.warning(f"Aucune colonne source trouvée pour {table_name}")
        return pd.DataFrame()
    
    # Créer un nouveau DataFrame avec les colonnes mappées
    df_mappe = df[colonnes_disponibles].copy()
    
    # Renommer les colonnes selon le mapping
    rename_dict = {}
    for i, source_col in enumerate(colonnes_disponibles):
        if i < len(target_cols):
            rename_dict[source_col] = target_cols[source_cols.index(source_col)]
    
    df_mappe.rename(columns=rename_dict, inplace=True)
    
    logging.info(f"Mapping {table_name}: {list(df.columns)} → {list(df_mappe.columns)}")
    return df_mappe

def extraire_donnees_sqlite():
    logging.info("Connexion SQLite : %s...", SQLITE_DB_PATH)
    conn = sqlite3.connect(SQLITE_DB_PATH)
    data = {
        "region": pd.read_sql("SELECT * FROM region", conn),
        "revendeur": pd.read_sql("SELECT * FROM revendeur", conn), 
        "produit": pd.read_sql("SELECT * FROM produit", conn),
        "production": pd.read_sql("SELECT * FROM production", conn)
    }
    conn.close()
    for table, df in data.items():
        logging.info("Table '%s': %d lignes", table, len(df))
    return data

def extraire_commandes_csv():
    logging.info("Extraction des commandes depuis : %s", CSV_PATH)
    if not os.path.exists(CSV_PATH):
        logging.warning("Fichier CSV non trouvé : %s", CSV_PATH)
        return pd.DataFrame()
    df = pd.read_csv(CSV_PATH)
    logging.info("%d lignes extraites du fichier CSV", len(df))
    return df

def connexion_mysql():
    return mysql.connector.connect(**MYSQL_CONFIG)

def charger_table_mysql(df, table_name, conn):
    if df.empty:
        logging.info("Aucune donnée à insérer dans '%s'", table_name)
        return
    
    cursor = conn.cursor()
    colonnes = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT IGNORE INTO {table_name} ({colonnes}) VALUES ({placeholders})"
    
    # Convertir les données en tuples, en gérant les valeurs None/NaN
    data = []
    for _, row in df.iterrows():
        row_data = []
        for val in row:
            if pd.isna(val):
                row_data.append(None)
            else:
                row_data.append(val)
        data.append(tuple(row_data))
    
    try:
        cursor.executemany(insert_sql, data)
        conn.commit()
        logging.info("✅ Chargement dans MySQL : '%s' → %d lignes", table_name, cursor.rowcount)
    except Exception as e:
        logging.error(f"❌ Erreur lors du chargement de {table_name}: {e}")
        conn.rollback()

def generer_commande_id(df_csv):
    """Génère des IDs uniques pour les commandes basés sur numero_commande"""
    commandes_uniques = df_csv[['numero_commande', 'commande_date', 'revendeur_id']].drop_duplicates()
    commandes_uniques['commande_id'] = range(1, len(commandes_uniques) + 1)
    
    # Fusionner avec le DataFrame original pour ajouter les IDs
    df_avec_id = df_csv.merge(commandes_uniques, on=['numero_commande', 'commande_date', 'revendeur_id'])
    return df_avec_id, commandes_uniques

def transformer_et_charger(donnees_sqlite, commandes_csv):
    conn = connexion_mysql()

    try:
        # === CHARGEMENT DES TABLES DE RÉFÉRENCE ===
        
        # Régions
        regions_mappees = mapper_colonnes(donnees_sqlite["region"], "Regions")
        charger_table_mysql(regions_mappees, "Regions", conn)
        
        # Revendeurs (sans email_contact car pas dans SQLite)
        revendeurs_mappees = mapper_colonnes(donnees_sqlite["revendeur"], "Revendeurs")
        charger_table_mysql(revendeurs_mappees, "Revendeurs", conn)
        
        # Produits
        produits_mappes = mapper_colonnes(donnees_sqlite["produit"], "Produits")
        charger_table_mysql(produits_mappes, "Produits", conn)
        
        # Productions
        productions_mappees = mapper_colonnes(donnees_sqlite["production"], "Productions")
        charger_table_mysql(productions_mappees, "Productions", conn)

        # === TRAITEMENT DES COMMANDES CSV ===
        
        if not commandes_csv.empty:
            # Générer les IDs de commandes
            df_avec_ids, commandes_uniques = generer_commande_id(commandes_csv)
            
            # Commandes principales
            commandes_mappees = mapper_colonnes(commandes_uniques, "Commandes")
            # Ajouter commande_id qui n'est pas dans le mapping automatique
            if not commandes_mappees.empty:
                commandes_mappees['commande_id'] = commandes_uniques['commande_id']
                # Réorganiser les colonnes
                colonnes_commandes = ['commande_id', 'numero_commande', 'date_commande', 'revendeur_id']
                colonnes_disponibles = [col for col in colonnes_commandes if col in commandes_mappees.columns]
                commandes_finales = commandes_mappees[colonnes_disponibles]
                charger_table_mysql(commandes_finales, "Commandes", conn)
            
            # Lignes de commandes
            df_lignes = df_avec_ids[['commande_id', 'product_id', 'quantity', 'unit_price']].copy()
            df_lignes['ligne_id'] = range(1, len(df_lignes) + 1)
            
            lignes_mappees = mapper_colonnes(df_lignes, "LignesCommande")
            if not lignes_mappees.empty:
                # Ajouter ligne_id qui n'est pas dans le mapping automatique
                lignes_mappees['ligne_id'] = df_lignes['ligne_id']
                # Réorganiser les colonnes
                colonnes_lignes = ['ligne_id', 'commande_id', 'produit_id', 'quantite', 'prix_unitaire_vente']  
                colonnes_disponibles = [col for col in colonnes_lignes if col in lignes_mappees.columns]
                lignes_finales = lignes_mappees[colonnes_disponibles]
                charger_table_mysql(lignes_finales, "LignesCommande", conn)

    except Exception as e:
        logging.error(f"❌ Erreur dans transformer_et_charger: {e}")
        conn.rollback()
    finally:
        conn.close()

def export_sql_complet():
    logging.info("Export SQL de la base complète...")
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{EXPORT_DIR}/distributech_export_{date_str}.sql"
    try:
        cmd = [
            "mysqldump",
            f"-u{MYSQL_USER}",
            f"-p{MYSQL_PASSWORD}",
            f"-h{MYSQL_HOST}",
            f"-P{MYSQL_PORT}",
            MYSQL_DB
        ]
        with open(output_file, "w", encoding='utf-8') as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True, check=True)
        logging.info("✅ Export SQL terminé : %s", output_file)
    except subprocess.CalledProcessError as e:
        logging.error("❌ Erreur export SQL : %s", e.stderr)
    except Exception as e:
        logging.error("❌ Erreur export SQL : %s", e)

def export_csv_stocks():
    logging.info("Export CSV des données de stocks...")
    try:
        conn = connexion_mysql()
        
        # Export des productions
        query_productions = """
        SELECT 
            p.production_id,
            p.product_id,
            pr.nom_produit,
            p.quantite_produite,
            p.date
        FROM Productions p
        LEFT JOIN Produits pr ON p.product_id = pr.produit_id
        ORDER BY p.date, p.product_id
        """
        df_productions = pd.read_sql(query_productions, conn)
        
        # Export des commandes détaillées
        query_commandes = """
        SELECT 
            c.commande_id,
            c.numero_commande,
            c.date_commande,
            r.nom_revendeur,
            rg.nom_region,
            lc.produit_id,
            p.nom_produit,
            lc.quantite,
            lc.prix_unitaire_vente,
            (lc.quantite * lc.prix_unitaire_vente) AS total_ligne
        FROM Commandes c
        LEFT JOIN LignesCommande lc ON c.commande_id = lc.commande_id
        LEFT JOIN Revendeurs r ON c.revendeur_id = r.revendeur_id  
        LEFT JOIN Regions rg ON r.region_id = rg.region_id
        LEFT JOIN Produits p ON lc.produit_id = p.produit_id
        ORDER BY c.date_commande, c.commande_id, lc.ligne_id
        """
        df_commandes = pd.read_sql(query_commandes, conn)
        
        # Calcul des stocks théoriques
        query_stocks = """
        SELECT 
            p.produit_id,
            p.nom_produit,
            COALESCE(prod.total_produit, 0) AS total_produit,
            COALESCE(cmd.total_commande, 0) AS total_commande,
            COALESCE(prod.total_produit, 0) - COALESCE(cmd.total_commande, 0) AS stock_theorique
        FROM Produits p
        LEFT JOIN (
            SELECT product_id, SUM(quantite_produite) AS total_produit
            FROM Productions 
            GROUP BY product_id
        ) prod ON p.produit_id = prod.product_id
        LEFT JOIN (
            SELECT produit_id, SUM(quantite) AS total_commande
            FROM LignesCommande
            GROUP BY produit_id
        ) cmd ON p.produit_id = cmd.produit_id
        ORDER BY p.produit_id
        """
        df_stocks = pd.read_sql(query_stocks, conn)
        
        conn.close()

        # Exports CSV
        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        output_productions = f"{EXPORT_DIR}/productions_{date_str}.csv"
        df_productions.to_csv(output_productions, index=False, encoding='utf-8')
        logging.info("✅ Export productions terminé : %s", output_productions)
        
        output_commandes = f"{EXPORT_DIR}/commandes_detaillees_{date_str}.csv"
        df_commandes.to_csv(output_commandes, index=False, encoding='utf-8')
        logging.info("✅ Export commandes terminé : %s", output_commandes)
        
        output_stocks = f"{EXPORT_DIR}/stocks_theoriques_{date_str}.csv"
        df_stocks.to_csv(output_stocks, index=False, encoding='utf-8')
        logging.info("✅ Export stocks terminé : %s", output_stocks)
        
    except Exception as e:
        logging.error("❌ Erreur export CSV : %s", e)

def afficher_resume():
    """Affiche un résumé des données chargées"""
    logging.info("=== RÉSUMÉ POST-CHARGEMENT ===")
    try:
        conn = connexion_mysql()
        cursor = conn.cursor()
        
        tables = ['Regions', 'Revendeurs', 'Produits', 'Productions', 'Commandes', 'LignesCommande']
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logging.info(f"📊 {table}: {count} enregistrements")
        
        conn.close()
    except Exception as e:
        logging.error(f"❌ Erreur affichage résumé: {e}")

# === MAIN ===

if __name__ == "__main__":
    logging.info("🚀 Démarrage du script ETL")

    # Étape 1 : Extraction
    logging.info("📥 Phase 1 : Extraction des données")
    donnees_sqlite = extraire_donnees_sqlite()
    commandes_csv = extraire_commandes_csv()

    # Étape 2 : Transformation + Chargement
    logging.info("🔄 Phase 2 : Transformation et chargement")
    transformer_et_charger(donnees_sqlite, commandes_csv)

    # Étape 3 : Résumé
    afficher_resume()

    # Étape 4 : Exports
    logging.info("📤 Phase 3 : Exports")
    export_sql_complet()
    export_csv_stocks()

    logging.info("✅ Script ETL terminé avec succès")
