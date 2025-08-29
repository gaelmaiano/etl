import pandas as pd
import mysql.connector
import os
from sqlalchemy import create_engine

# --- Configuration et vérifications initiales ---

# Nom du fichier CSV à traiter
csv_filename = 'commande_revendeur_tech_express.csv'

# Paramètres de connexion à la base de données MySQL
db_config = {
    'host': '127.0.0.1', 
    'port': '3306' ,
    'user': 'appuser',
    'password': 'example_password' ,
    'database': 'distributech_db'
}

# Et modifiez la création de l'engine :
engine = create_engine(
    f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}?auth_plugin=mysql_native_password",
    echo=False
)

# --- Début du script ---

# Vérifiez si le fichier CSV existe
if not os.path.exists(csv_filename):
    print(f"Erreur : Le fichier '{csv_filename}' est introuvable. Assurez-vous qu'il est dans le même répertoire.")
else:
    try:
        # 1. Lecture du fichier CSV avec pandas
        data = pd.read_csv(csv_filename)
        print("Fichier CSV lu avec succès. Création des tables en cours...")

        # 2. Création des DataFrames pour chaque table relationnelle
        resellers_df = data[['revendeur_id', 'region_id']].drop_duplicates()
        products_df = data[['product_id', 'unit_price']].drop_duplicates()
        orders_df = data[['numero_commande', 'commande_date', 'revendeur_id']].drop_duplicates()
        order_items_df = data[['numero_commande', 'product_id', 'quantity', 'unit_price']]

        # 3. Connexion à la base de données MySQL
        engine = create_engine(
            f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )

        print("Connexion à la base de données MySQL établie.")

        # 4. Écriture des DataFrames dans des tables SQL
        resellers_df.to_sql(name='resellers', con=engine, if_exists='replace', index=False)
        products_df.to_sql(name='products', con=engine, if_exists='replace', index=False)
        orders_df.to_sql(name='orders', con=engine, if_exists='replace', index=False)
        order_items_df.to_sql(name='order_items', con=engine, if_exists='replace', index=False)
        
        print("Toutes les tables ont été créées et remplies dans la base de données MySQL.")

        # 5. Vérification des données (optionnel)
        read_orders = pd.read_sql("SELECT * FROM orders LIMIT 5", con=engine)
        print("\nAperçu des 5 premières lignes de la table 'orders':")
        print(read_orders)

    except mysql.connector.Error as e:
        print(f"Erreur MySQL: {e}")
        print("Vérifiez que votre conteneur Docker MySQL est bien démarré, que l'utilisateur 'appuser' a été créé et que ses permissions sont correctes.")
    except Exception as e:
        print(f"Une erreur inattendue est survenue : {e}")