#!/bin/bash

# Aller dans le répertoire du projet
cd /home/user/iadev/distributech || {
    echo "❌ Échec : répertoire introuvable"
    exit 1
}

# Activer l'environnement virtuel
source venv/bin/activate || {
    echo "❌ Échec : impossible d'activer l'environnement virtuel"
    exit 1
}

# Lancer le script Python
python distributech_etl_improved.py

# Optionnel : log de l'exécution
echo "✅ ETL exécuté le $(date)" >> etl_execution.log
