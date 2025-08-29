import logging
import sqlalchemy
import pandas as pd

def charger_donnees_mysql(df: pd.DataFrame, table_name: str, engine: sqlalchemy.engine.base.Engine):
    try:
        logging.info(f"Préparation du chargement vers la table `{table_name}`")

        # Convertir les types NumPy en types Python natifs
        df = df.astype(object)

        # Écriture dans la table (append = insère, ne remplace pas)
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

        logging.info(f"✅ Chargement terminé pour la table `{table_name}`")

    except Exception as e:
        logging.error(f"❌ Erreur lors du chargement vers MySQL - Table `{table_name}`: {e}")
        raise  # pour que l’ETL général sache qu’il y a une erreur
