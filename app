
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

# --- Configuration MySQL ---
MYSQL_USER = 'appuser'
MYSQL_PASSWORD = 'example_password'
MYSQL_HOST = 'localhost'
MYSQL_PORT = '3307'
MYSQL_DB = 'distributech_db'

engine = create_engine(f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")

st.title("📦 Tableau de bord Distributech")

# --- Section 1 : Visualisation des données
st.header("1. État des stocks")

query_stock = """
SELECT
    p.produit_id,
    p.nom_produit,
    COALESCE(SUM(prod.quantite_produite), 0) AS quantite_produite,
    COALESCE(SUM(lc.quantite), 0) AS quantite_vendue,
    COALESCE(SUM(prod.quantite_produite), 0) - COALESCE(SUM(lc.quantite), 0) AS stock_disponible
FROM Produits p
LEFT JOIN Productions prod ON p.produit_id = prod.product_id
LEFT JOIN LignesCommande lc ON p.produit_id = lc.produit_id
GROUP BY p.produit_id, p.nom_produit
ORDER BY p.produit_id;
"""

df_stock = pd.read_sql(query_stock, engine)
st.dataframe(df_stock)

# --- Section 2 : Graphique
st.header("2. Visualisation graphique")

option = st.selectbox("Choisir une variable à visualiser :", ["quantite_produite", "quantite_vendue", "stock_disponible"])

st.bar_chart(df_stock.set_index("nom_produit")[option])

# --- Section 3 : Uploader un fichier CSV
st.header("3. Upload d'un nouveau fichier de commandes")

uploaded_file = st.file_uploader("Uploader un fichier CSV", type="csv")
if uploaded_file:
    df_new = pd.read_csv(uploaded_file)
    st.write("Aperçu du fichier :")
    st.dataframe(df_new)

    if st.button("⚙️ Lancer le traitement sur ce fichier"):
        # Ici tu peux appeler une fonction de traitement comme extract + load
        st.success("Traitement lancé (à connecter avec ton ETL)")

# --- Section 4 : Export
st.header("4. Télécharger l'état des stocks")
if st.button("📤 Générer l'export CSV"):
    df_stock.to_csv("etat_export.csv", index=False)
    st.download_button("Télécharger", "etat_export.csv", file_name="etat_stocks.csv")

