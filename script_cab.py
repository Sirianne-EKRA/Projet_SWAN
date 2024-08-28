import os
import pandas as pd
import openpyxl
import time
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from datetime import datetime, timedelta

def extract_and_update_cab(input_file_path, output):
    start_time = time.time()
    dernier_nom_court = None
    # Charger le fichier CSV existant
    if  os.path.exists(output):
        df_cab = pd.read_csv(output,sep='|')
        df_cab = df_cab.loc[:, ~df_cab.columns.duplicated()]

    else:
        df_cab = pd.DataFrame()
    
    # Liste des fichiers Excel dans le répertoire d'entrée
    fichiers_excel = [f for f in os.listdir(input_file_path) if f.endswith(".xlsx") and f.startswith("SWAN GOS Changement CAB")]

    if fichiers_excel:
        # Trier les fichiers par date de modification (le plus récent en premier)
        fichiers_excel.sort(key=lambda x: os.path.getmtime(os.path.join(input_file_path, x)), reverse=True)
        fichier_plus_recent = fichiers_excel[0]
        chemin_fichier_entree = os.path.join(input_file_path, fichier_plus_recent)

        # Charger la feuille spécifiée du fichier Excel dans un DataFrame 
        operations_par_eds = pd.read_excel(chemin_fichier_entree, sheet_name='Opérations par EDS', header=1 ,engine='openpyxl')

        #TRAITEMENT DE LA FEUILLE OPERATIONS PAR EDS
        operations_par_eds = operations_par_eds.drop(columns=operations_par_eds.columns[0])
        operations_par_eds = operations_par_eds.dropna(how='all')

        for i, row in operations_par_eds.iterrows():
            # Si la colonne 'Nom court EDS pilote' (Nom court EDS pilote) contient une nouvelle valeur (pas NaN)
            if pd.notna(row['Nom court EDS pilote']) and row['Nom court EDS pilote'] != 'Nom court EDS pilote':
                dernier_nom_court = row['Nom court EDS pilote']
        
            # Remplir les NaN dans la colonne 'Nom court EDS pilote' avec la dernière valeur non-NaN
            elif pd.isna(row['Nom court EDS pilote']):
                operations_par_eds.at[i, 'Nom court EDS pilote'] = dernier_nom_court

        # Convertir la colonne 'Date création' en type datetime, avec des erreurs converties en NaT (Not a Time)
        operations_par_eds['Date création'] = pd.to_datetime(operations_par_eds['Date création'], errors='coerce')
        # Supprimer les lignes où 'Date création' est NaT (c'est-à-dire où la conversion en date a échoué)
        operations_par_eds = operations_par_eds.dropna(subset=['Date création'])
        operations_par_eds = operations_par_eds[operations_par_eds['Date création'].dt.year >= 2024]

        #TRAITEMENT DE LA FEUILLE LISTE DES OPERATIONS OASTD
        df_extrait = pd.read_excel(chemin_fichier_entree, sheet_name='Liste des opérations OASTD',  skiprows=3 ,engine='openpyxl')
        df_extrait = df_extrait.dropna(how='all')
        df_extrait = df_extrait.iloc[:, 1:]
        # Convertir la colonne 'Date création' en type datetime, avec des erreurs converties en NaT (Not a Time)
        df_extrait['Date création'] = pd.to_datetime(df_extrait['Date création'], errors='coerce')
        # Supprimer les lignes où 'Date création' est NaT (c'est-à-dire où la conversion en date a échoué)
        df_extrait = df_extrait.dropna(subset=['Date création'])
        df_extrait = df_extrait[df_extrait['Date création'].dt.year >= 2024]


        # Fusionner df_extrait avec operations_par_eds sur 'N° opération'
        df_extrait = pd.merge(df_extrait, operations_par_eds[['N° opération', 'Nom court EDS pilote']], on='N° opération', how='left')
        df_extrait = pd.merge(df_extrait, operations_par_eds[['N° opération', 'Libellé modèle opération']], on='N° opération', how='left')
        df_extrait = pd.merge(df_extrait, operations_par_eds[['N° opération', 'Nombre operations traitées hors delais']], on='N° opération', how='left')

        df_extrait['Date_de_Fin'] = df_extrait['Date fin réelle'].fillna(df_extrait['Date fin révisée']).fillna(df_extrait['Date fin initiale'])
        

    for fichier in fichiers_excel[1:]:
        chemin_fichier = os.path.join(input, fichier)
        os.remove(chemin_fichier)

    if df_cab.empty:
        df_cab = df_extrait.copy()
    else:
        for _, row in df_extrait.iterrows():
            numero_op = row['N° opération']
            if numero_op in df_cab['N° opération'].values:
                indice_ligne = df_cab.index[df_cab['N° opération'] == numero_op].tolist()[0]
                # Mettre à jour la ligne existante dans df_cab avec les valeurs de row
                df_cab.iloc[indice_ligne] = row
            else:
                # Concaténer la ligne de df_extrait à df_cab
                df_cab = pd.concat([df_cab, row.to_frame().T], ignore_index=True)


    if df_cab is not None:
    # Enregistrer le DataFrame df_cab mis à jour
        df_cab = df_cab.drop_duplicates()
        df_cab.to_csv(output, index=False, sep='|', encoding='UTF-8')
        df_cab.to_excel('Changements_CAB.xlsx', index=False, engine='openpyxl')
        end_time = time.time()
        execution_time_seconds = end_time - start_time
        execution_time_minutes = execution_time_seconds / 60
        print("-------------------------------------------------------------------------")
        print("Le fichier de SWAN GOS Changement CAB le plus récent :", fichier_plus_recent)
        print("Nombre de lignes de Changements_CAB :", df_cab.shape[0], "Nombre de colonnes de Changements_CAB:", df_cab.shape[1])
        print(f"Extraction, traitement et mise à jour terminés faits en {execution_time_minutes} Minutes le {datetime.today()}.")
    else:
        print("Le chargement du fichier CSV existant a échoué.")


# Exemple d'utilisation de la fonction
input_file_path = '/home/ajunior/' 
output = '/home/ajunior/Swan_project/Temporary_data/Requetes.csv'

extract_and_update_cab(input_file_path, output)