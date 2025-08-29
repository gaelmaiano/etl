import sqlite3
import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
import sys
from datetime import datetime
import random

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLProcessor:
    def __init__(self):
        self.mysql_config = {
            'host': 'localhost',
            'port': 3307,
            'database': 'distributech_db',
            'user': 'appuser',
            'password': 'example_password'
        }
        self.sqlite_path = "./data/base_stock.sqlite"
        self.csv_path = "commande_revendeur_tech_express.csv"
        self.mysql_conn = None
        self.sqlite_conn = None

    def connect_mysql(self):
        try:
            self.mysql_conn = mysql.connector.connect(**self.mysql_config)
            logger.info("Connexion MySQL établie")
            return True
        except Error as e:
            logger.error(f"Erreur connexion MySQL: {e}")
            return False

    def connect_sqlite(self):
        try:
            self.sqlite_conn = sqlite3.connect(self.sqlite_path)
            logger.info("Connexion SQLite établie")
            return True
        except Exception as e:
            logger.error(f"Erreur connexion SQLite: {e}")
            return False

    def extract_sqlite_data(self):
        logger.info("Extraction des données SQLite...")
        data = {}
        try:
            data['regions'] = pd.read_sql_query("SELECT * FROM region", self.sqlite_conn)
            data['revendeurs'] = pd.read_sql_query("SELECT * FROM revendeur", self.sqlite_conn)
            data['produits'] = pd.read_sql_query("SELECT * FROM produit", self.sqlite_conn)
            data['production'] = pd.read_sql_query("SELECT * FROM production", self.sqlite_conn)
            logger.info(f"Données extraites: {len(data)} tables")
            return data
        except Exception as e:
            logger.error(f"Erreur extraction SQLite: {e}")
            raise

    def extract_csv_data(self):
        logger.info("Extraction des données CSV...")
        try:
            csv_data = pd.read_csv(self.csv_path)
            logger.info(f"CSV chargé: {len(csv_data)} lignes")
            return csv_data
        except Exception as e:
            logger.error(f"Erreur lecture CSV: {e}")
            return None

    def transform_data(self, sqlite_data, csv_data):
        logger.info("Transformation des données...")
        transformed_data = {}

        regions = sqlite_data['regions'].rename(columns={'region_name': 'nom_region'})
        transformed_data['regions'] = regions

        revendeurs = sqlite_data['revendeurs'].rename(columns={'revendeur_name': 'nom_revendeur'})
        revendeurs['contact_email'] = (
            revendeurs['nom_revendeur']
            .astype(str).str.lower().str.replace(' ', '', regex=False)
            .str.replace('[^a-z0-9]', '', regex=True) + '@distributech.com'
        )
        transformed_data['revendeurs'] = revendeurs

        produits = sqlite_data['produits'].rename(columns={
            'product_id': 'produit_id',
            'product_name': 'nom_produit',
            'cout_unitaire': 'prix_unitaire'
        })
        produits['description'] = 'Produit électronique'
        transformed_data['produits'] = produits

        production = sqlite_data['production']
        stocks_data = []
        random.seed(42)

        for _, prod in production.iterrows():
            base_quantity = max(1, prod['quantity'] // len(revendeurs))
            for _, rev in revendeurs.iterrows():
                quantite = random.randint(0, base_quantity + 2)
                stocks_data.append({
                    'revendeur_id': int(rev['revendeur_id']),
                    'produit_id': int(prod['product_id']),
                    'quantite': quantite
                })
        transformed_data['stocks'] = pd.DataFrame(stocks_data)

        if csv_data is not None and not csv_data.empty:
            try:
                montants_par_commande = {}
                for _, row in csv_data.iterrows():
                    cmd = row['numero_commande']
                    montant = float(row['quantity']) * float(row['unit_price'])
                    montants_par_commande[cmd] = montants_par_commande.get(cmd, 0) + montant

                commandes_uniques = csv_data.drop_duplicates(subset=['numero_commande'])
                commandes = commandes_uniques[['numero_commande', 'commande_date', 'revendeur_id']].copy()
                commandes['montant_total'] = commandes['numero_commande'].map(montants_par_commande)
                commandes = commandes.rename(columns={'commande_date': 'date_commande'})
                commandes['date_commande'] = pd.to_datetime(commandes['date_commande']).dt.strftime('%Y-%m-%d %H:%M:%S')
                commandes['etat_commande'] = 'En cours'
                commandes['commande_id'] = range(3, len(commandes) + 3)
                transformed_data['commandes'] = commandes

                lignes = csv_data.rename(columns={
                    'product_id': 'produit_id',
                    'quantity': 'quantite',
                    'unit_price': 'prix_unitaire_vente'
                }).copy()
                mapping = dict(zip(commandes['numero_commande'], commandes['commande_id']))
                lignes['commande_id'] = lignes['numero_commande'].map(mapping)
                lignes['ligne_id'] = range(4, len(lignes) + 4)
                lignes = lignes[['ligne_id', 'commande_id', 'produit_id', 'quantite', 'prix_unitaire_vente']]
                lignes = lignes.astype({'commande_id': int, 'produit_id': int, 'quantite': int, 'prix_unitaire_vente': float})
                transformed_data['lignes_commande'] = lignes

            except Exception as e:
                logger.error(f"Erreur transformation CSV: {e}")

        logger.info("Transformation terminée")
        return transformed_data

    def load_data_to_mysql(self, data):
        logger.info("Chargement des données dans MySQL...")
        cursor = self.mysql_conn.cursor()
        try:
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

            if 'regions' in data:
                cursor.execute("DELETE FROM Regions WHERE region_id > 3")
                for _, row in data['regions'].iterrows():
                    cursor.execute(
                        "INSERT IGNORE INTO Regions (region_id, nom_region) VALUES (%s, %s)",
                        (row['region_id'], row['nom_region'])
                    )

            if 'revendeurs' in data:
                cursor.execute("DELETE FROM Revendeurs WHERE revendeur_id > 3")
                for _, row in data['revendeurs'].iterrows():
                    cursor.execute(
                        "INSERT IGNORE INTO Revendeurs (revendeur_id, nom_revendeur, contact_email, region_id) VALUES (%s, %s, %s, %s)",
                        (row['revendeur_id'], row['nom_revendeur'], row['contact_email'], row['region_id'])
                    )

            if 'produits' in data:
                cursor.execute("DELETE FROM Produits WHERE produit_id > 3")
                for _, row in data['produits'].iterrows():
                    cursor.execute(
                        "INSERT IGNORE INTO Produits (produit_id, nom_produit, description, prix_unitaire) VALUES (%s, %s, %s, %s)",
                        (row['produit_id'], row['nom_produit'], row['description'], row['prix_unitaire'])
                    )

            if 'stocks' in data:
                cursor.execute("DELETE FROM Stocks WHERE stock_id > 6")
                for _, row in data['stocks'].iterrows():
                    if row['quantite'] > 0:
                        cursor.execute(
                            "INSERT INTO Stocks (revendeur_id, produit_id, quantite) VALUES (%s, %s, %s)",
                            (row['revendeur_id'], row['produit_id'], row['quantite'])
                        )

            if 'commandes' in data:
                cursor.execute("DELETE FROM Commandes WHERE commande_id > 2")
                for _, row in data['commandes'].iterrows():
                    cursor.execute(
                        "INSERT INTO Commandes (commande_id, revendeur_id, date_commande, montant_total, etat_commande) VALUES (%s, %s, %s, %s, %s)",
                        (row['commande_id'], row['revendeur_id'], row['date_commande'], row['montant_total'], row['etat_commande'])
                    )

            if 'lignes_commande' in data:
                cursor.execute("DELETE FROM LignesCommande WHERE ligne_id > 3")
                for _, row in data['lignes_commande'].iterrows():
                    cursor.execute(
                        "INSERT INTO LignesCommande (ligne_id, commande_id, produit_id, quantite, prix_unitaire_vente) VALUES (%s, %s, %s, %s, %s)",
                        (row['ligne_id'], row['commande_id'], row['produit_id'], row['quantite'], row['prix_unitaire_vente'])
                    )

            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            self.mysql_conn.commit()
            logger.info("Toutes les données ont été chargées avec succès")
        except Error as e:
            logger.error(f"Erreur lors du chargement: {e}")
            self.mysql_conn.rollback()
            raise
        finally:
            cursor.close()

    def run_etl(self):
        logger.info("Début du processus ETL")
        try:
            if not self.connect_mysql() or not self.connect_sqlite():
                return False
            sqlite_data = self.extract_sqlite_data()
            csv_data = self.extract_csv_data()
            transformed_data = self.transform_data(sqlite_data, csv_data)
            self.load_data_to_mysql(transformed_data)
            logger.info("Processus ETL terminé avec succès")
            return True
        except Exception as e:
            logger.error(f"Erreur dans le processus ETL: {e}")
            return False
        finally:
            if self.mysql_conn:
                self.mysql_conn.close()
            if self.sqlite_conn:
                self.sqlite_conn.close()

def main():
    etl = ETLProcessor()
    if etl.run_etl():
        print("✅ ETL terminé avec succès!")
    else:
        print("❌ Erreur lors de l'exécution de l'ETL")

if __name__ == "__main__":
    main()
