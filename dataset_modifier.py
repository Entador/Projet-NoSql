import mysql.connector
import pandas as pd

# Informations de connexion à la base de données
host = "localhost"
user = "thomas"
password = "thomas"
database = "projet"

# Chemin vers le fichier CSV
chemin_csv1 = ""
chemin_csv2 = ""
# Connexion à la base de données
connexion = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

# Création d'un objet curseur pour exécuter des requêtes SQL
curseur = connexion.cursor()

# Lecture du fichier CSV avec pandas
df_cons = pd.read_csv(chemin_csv1, delimiter=";")
df_test = df_cons.iloc[:10]

# Supposons que votre DataFrame s'appelle donnees_csv
df_cons['adresse_complete'] = df_cons['adresse'] + ', ' + df_cons['nom_commune']
# Supprimez la colonne adresse si nécessaire
df_cons.drop(['adresse', 'numero_de_voie', 'type_de_voie', 'libelle_de_voie'], axis=1, inplace=True)


# Créez une nouvelle colonne avec des identifiants uniques basés sur la valeur de l'adresse complète
df_cons['id_unique'] = df_cons['adresse_complete'].apply(lambda x: hash(x))
df_cons.to_csv('cons_maj.csv', index=False, sep=';')
# Affichez le DataFrame mis à jour


df_test['adresse_complete'] = df_test['adresse'] + ', ' + df_test['nom_commune']
# Supprimez la colonne adresse si nécessaire
df_test.drop(['adresse', 'numero_de_voie', 'type_de_voie', 'libelle_de_voie'], axis=1, inplace=True)


# Créez une nouvelle colonne avec des identifiants uniques basés sur la valeur de l'adresse complète
df_test['id_unique'] = df_test['adresse_complete'].apply(lambda x: hash(x))
df_test.to_csv('test_maj.csv', index=False, sep=';')
# Affichez le DataFrame mis à jour