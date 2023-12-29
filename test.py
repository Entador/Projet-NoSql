import pandas as pd
import mysql.connector
from mysql.connector import Error

# Spécifiez le chemin du fichier CSV
csv_file_path = '/home/poirrier/Documents/traitement_et_donnees_reparties/Projet_nosql/consommation-annuelle-residentielle-par-adresse.csv'

# Spécifiez les détails de la connexion MySQL
mysql_config = {
    'host': 'localhost',
    'database': 'db',
    'user': 'root',
    'password': ''
}

# Lire le fichier CSV dans un DataFrame
df = pd.read_csv(csv_file_path, delimiter=';')

# Fonction pour créer une connexion MySQL
def create_connection():
    connection = None
    try:
        connection = mysql.connector.connect(**mysql_config)
        if connection.is_connected():
            print("Connecté à MySQL Server version", connection.get_server_info())
            return connection
    except Error as e:
        print("Erreur lors de la connexion à MySQL:", e)
    return None

# Fonction pour créer une table MySQL et insérer les données
def create_table_and_insert_data(connection, table_name):
    try:
        cursor = connection.cursor()

        # Créer la table
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col} VARCHAR(255)' for col in df.columns])})"
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' créée avec succès.")

        # Insérer les données dans la table
        for index, row in df.iterrows():
            insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"
            cursor.execute(insert_query, tuple(row))

        connection.commit()
        print("Données insérées avec succès.")

    except Error as e:
        print("Erreur lors de la création de la table et de l'insertion des données:", e)

# Étape principale
if __name__ == "__main__":
    connection = create_connection()
    if connection:
        table_name = 'votre_table'
        create_table_and_insert_data(connection, table_name)
        connection.close()
        print("Connexion MySQL fermée.")
