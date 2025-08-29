
import pandas as pd
import mysql.connector
import logging
import os

# --- Configuration MySQL ---
MYSQL_USER = 'appuser'
MYSQL_PASSWORD = 'example_password'
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3307'
MYSQL_DB = 'distributech_db'

# --- Configuration du logger ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def generate_stock_report_csv():
    """
    Se connecte à la base de données MySQL, calcule l'état des stocks
    à partir des productions et exporte le résultat dans un CSV.
    """
    logging.info("🚀 Démarrage de la génération du rapport de stock CSV.")
    conn = None
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DB,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )

        if conn.is_connected():
            logging.info("Connexion à la base de données MySQL établie.")
            
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
            
            output_csv_path = 'etat_des_stocks.csv'
            df_stock.to_csv(output_csv_path, index=False)
            logging.info(f"✅ Rapport de stock exporté avec succès vers '{output_csv_path}'")
            logging.info(f"Contenu du fichier '{output_csv_path}':")
            print(df_stock.to_string(index=False)) # Affiche le contenu dans la console

        else:
            logging.error("❌ Échec de la connexion à la base de données MySQL.")

    except mysql.connector.Error as e:
        logging.error(f"❌ Erreur MySQL lors de la génération du rapport de stock : {e}")
    except Exception as e:
        logging.error(f"❌ Une erreur inattendue est survenue : {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            logging.info("Connexion MySQL fermée.")

if __name__ == "__main__":
    generate_stock_report_csv()