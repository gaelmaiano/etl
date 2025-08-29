def main():
    logging.info("🚀 Démarrage du processus ETL")
    engine = None
    try:
        # Connexion MySQL
        mysql_url = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
        engine = create_engine(mysql_url)
        
        with engine.connect() as connection:
            logging.info("Connexion à la base de données MySQL établie.")
            
            # --- Désactiver temporairement les vérifications de clés étrangères ---
            logging.info("Désactivation des vérifications de clés étrangères...")
            connection.execute(types.text("SET FOREIGN_KEY_CHECKS = 0;"))
            connection.commit() # Important pour que le changement soit appliqué
            logging.info("Vérifications de clés étrangères désactivées.")

        # --- Extraction ---
        df_csv = extract_csv(CSV_PATH)
        sqlite_data = extract_sqlite(SQLITE_DB_PATH)

        # --- Transformation et Chargement ---
        # L'ordre de chargement est important même avec FOREIGN_KEY_CHECKS = 0
        # Pour 'replace', l'ordre de DROP est inversé par SQLAlchemy.
        # Donc, avec la désactivation, on n'a pas besoin de se soucier de l'ordre de chargement.
        # Les tables seront recréées proprement.

        # Données des régions
        df_regions = sqlite_data.get('region', pd.DataFrame()).rename(columns={'region_id': 'id_region', 'region_name': 'nom_region'})
        if not df_regions.empty:
            load_to_mysql(df_regions, 'Regions', engine)

        # Données des revendeurs
        df_revendeurs = sqlite_data.get('revendeur', pd.DataFrame()).rename(columns={'revendeur_id': 'id_revendeur', 'revendeur_name': 'nom_revendeur', 'region_id': 'id_region'})
        df_revendeurs['email_contact'] = df_revendeurs['nom_revendeur'].apply(lambda x: f"{x.lower().replace(' ', '')}@exemple.com")
        if not df_revendeurs.empty:
            load_to_mysql(df_revendeurs, 'Revendeurs', engine)

        # Données des produits
        df_produits = sqlite_data.get('produit', pd.DataFrame()).rename(columns={'product_id': 'id_produit', 'product_name': 'nom_produit', 'cout_unitaire': 'prix_unitaire'})
        if not df_produits.empty:
            load_to_mysql(df_produits, 'Produits', engine)
            
        # Données de production
        df_production = sqlite_data.get('production', pd.DataFrame()).rename(columns={'production_id': 'id_production', 'product_id': 'id_produit', 'quantity': 'quantite_produite', 'date_production': 'date'})
        if not df_production.empty:
             load_to_mysql(df_production, 'Productions', engine)

        # Transformation et chargement des données CSV (commandes)
        df_commandes_csv = df_csv.rename(columns={
            'numero_commande': 'numero_commande',
            'commande_date': 'date_commande',
            'revendeur_id': 'id_revendeur',
            'region_id': 'id_region',
            'product_id': 'id_produit',
            'quantity': 'quantite',
            'unit_price': 'prix_unitaire_vente'
        })
        
        if 'id_revendeur' in df_commandes_csv.columns:
            df_commandes_csv['id_revendeur'] = pd.to_numeric(df_commandes_csv['id_revendeur'], errors='coerce').fillna(0).astype(int)
        
        df_commandes_csv['commande_id_unique'] = df_commandes_csv.groupby(['numero_commande', 'date_commande', 'id_revendeur']).ngroup() + 1
        
        df_commandes = df_commandes_csv[['commande_id_unique', 'numero_commande', 'date_commande', 'id_revendeur']].drop_duplicates()
        df_commandes = df_commandes.rename(columns={'commande_id_unique': 'commande_id'})
        df_commandes['date_commande'] = pd.to_datetime(df_commandes['date_commande'])
        if not df_commandes.empty:
            load_to_mysql(df_commandes, 'Commandes', engine)

        df_lignes_commande = df_commandes_csv[['commande_id_unique', 'id_produit', 'quantite', 'prix_unitaire_vente']]
        df_lignes_commande['ligne_id'] = range(1, len(df_lignes_commande) + 1)
        df_lignes_commande = df_lignes_commande.rename(columns={'commande_id_unique': 'commande_id'})
        df_lignes_commande = df_lignes_commande[['ligne_id', 'commande_id', 'id_produit', 'quantite', 'prix_unitaire_vente']]
        
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
                    connection.execute(types.text("SET FOREIGN_KEY_CHECKS = 1;"))
                    connection.commit()
                    logging.info("Vérifications de clés étrangères réactivées.")
            except Exception as e:
                logging.error(f"Erreur lors de la réactivation des clés étrangères : {e}")
            finally:
                engine.dispose() # S'assurer que les connexions sont fermées

if __name__ == "__main__":
    main()