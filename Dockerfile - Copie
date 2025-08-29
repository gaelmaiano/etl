
# Utiliser l'image de base Alpine, qui est très légère
FROM alpine:latest

# Exécuter une commande pour mettre à jour les dépôts et installer sqlite
# La commande 'apk add' est le gestionnaire de paquets d'Alpine
RUN apk update && apk add sqlite

# Définir un répertoire de travail pour la base de données
# Cela rend les commandes plus faciles et organise les fichiers
WORKDIR /data

# Définir la commande par défaut à exécuter quand le conteneur démarre.
# Cela lance l'interpréteur de commandes de sqlite3,
# ce qui rend le conteneur interactif par défaut.
CMD ["sqlite3"]