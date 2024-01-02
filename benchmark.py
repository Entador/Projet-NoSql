import mysql.connector
import pandas as pd
import time
from chrono import Chrono

# Informations de connexion à la base de données
host = "localhost"
user = "thomas"
password = "thomas"
database = "projet"

# Connexion à la base de données
connexion = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)

# Création d'un objet curseur pour exécuter des requêtes SQL
curseur = connexion.cursor(buffered=True)




#####################################################################
#------------------------- Création des tables ---------------------#
#####################################################################

print("Création des tables...")


# Suppression des tables si elles existent déjà pour supprimer les données
#curseur.execute(f"DROP TABLE IF EXISTS Consommation;")
#curseur.execute(f"DROP TABLE IF EXISTS Adresse;")
#connexion.commit()


# Création de la table Adresse
nom_table = "Adresse"
curseur.execute(f"""CREATE TABLE IF NOT EXISTS {nom_table}(
                id INT AUTO_INCREMENT PRIMARY KEY, 
                adresse_complete VARCHAR(255) UNIQUE, 
                commune VARCHAR(255), 
                segment_client VARCHAR(30), 
                nb_logements INT);
               """)


# Création de la table Consommation
nom_table = "Consommation"
curseur.execute(f"""
    CREATE TABLE IF NOT EXISTS {nom_table} (
        PRIMARY KEY (id_adresse, annee),  -- Primary key
        id_adresse INT,
        consommation_mwh DOUBLE,
        annee INT,
        FOREIGN KEY (id_adresse) REFERENCES Adresse(id)
    );""")

connexion.commit()





chemin_csv = "/home/poirrier/Documents/traitement_et_donnees_reparties/Projet-NoSql/cons_maj.csv"
# Lecture du fichier CSV avec pandas
donnees_csv = pd.read_csv(chemin_csv, delimiter=";")



#####################################################################
#------------------------- Insertion des données -------------------#
#####################################################################


print("Insertion des données...")

#Initialisez le chrono
chrono = Chrono()

chrono.start()
chrono.pause()


for index, row in donnees_csv.iterrows():
    # Récupérez les valeurs de chaque colonne
    adresse_complete = row['adresse_complete']
    commune = row['nom_commune']
    segment_client = row['segment_de_client']
    nb_logements = row['nombre_de_logements']


    chrono.resume()
    requete_insertion = f"""INSERT IGNORE INTO Adresse 
                            (adresse_complete, commune, segment_client, nb_logements) 
                            VALUES ("{adresse_complete}", "{commune}", 
                                    "{segment_client}", {nb_logements});
                        """

    #Exécutez la requête
    curseur.execute(requete_insertion)
    chrono.pause()

# Arrêtez le chrono
elapsed_time = chrono.stop()
print(f"Insertion table Adresse: {elapsed_time} secondes.")

connexion.commit()



chrono.start()
chrono.pause()

 
for index, row in donnees_csv.iterrows():
    # Assuming you have columns 'adresse_complete', 'commune', 'segment_client' in your CSV
    adresse_complete = row['adresse_complete']
    commune = row['nom_commune']
    segment_client = row['segment_de_client']
    consommation_mwh = row['consommation_annuelle_totale_de_l_adresse_mwh']  # Assuming you have a column 'consommation_mwh'
    annee = row['annee']  # Assuming you have a column 'annee'
        

    requete_insertion = f"""INSERT IGNORE INTO Consommation 
                            (consommation_mwh, annee, id_adresse)
                            VALUES (
                                {consommation_mwh},
                                {annee},
                                (SELECT id FROM Adresse WHERE adresse_complete = "{adresse_complete}")
                                )
                            """
        
    # Execute the SQL query
    chrono.resume()
    curseur.execute(requete_insertion)
    chrono.pause()


elapsed_time = chrono.stop()
print(f"Insertion table Consommation: {elapsed_time} secondes.")

# Commit the changes to the database
connexion.commit()


#####################################################################
#------------------------- Requêtes --------------------------------#
#####################################################################

print("Requêtes...")

nb_requetes = 0

chrono.start()
# Select data from Adresse table
curseur.execute("SELECT * FROM Adresse;")
chrono.pause()

nb_requetes += curseur.rowcount
results = curseur.fetchall()

# Select data from Consommation table
chrono.resume()
curseur.execute("SELECT * FROM Consommation;")
chrono.pause()

nb_requetes += curseur.rowcount
curseur.fetchall()

chrono.resume()
curseur.execute("""
    SELECT Adresse.adresse_complete, Adresse.commune, Consommation.consommation_mwh, Consommation.annee
    FROM Adresse
    INNER JOIN Consommation ON Adresse.id = Consommation.id_adresse;
""")
elapsed_time = chrono.stop()


nb_requetes += curseur.rowcount
curseur.fetchall()


print(f"{elapsed_time} secondes pour {nb_requetes} requêtes.")



#####################################################################
#------------------------- Mise à jour -----------------------------#
#####################################################################

print("Mise à jour...") 
nb_update = 0

chrono.start()
# Update data in Adresse table for all records where commune is 'Paris'
curseur.execute("""
    UPDATE Adresse
    SET nb_logements = 20
    WHERE commune = 'Paris';
""")
chrono.pause()

nb_update += curseur.rowcount

chrono.resume()
# Update data in Adresse table for records with more than 10 logements and in Paris
curseur.execute("""
    UPDATE Adresse
    SET nb_logements = nb_logements + 5
    WHERE nb_logements > 10 AND commune = 'Paris';
""")
elapsed_time = chrono.stop()
connexion.commit()

nb_update += curseur.rowcount

print(f"{elapsed_time} secondes pour {nb_update} update.")



# Close the cursor and connection
curseur.close()
connexion.close()
