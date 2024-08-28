import os
import pandas as pd
import openpyxl
import time
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from datetime import datetime, timedelta

def extract_and_update_req(input_file_path, output):
    start_time = time.time()
    dernier_nom_court = None
    # Charger le fichier CSV existant
    if  os.path.exists(output):
        df_req = pd.read_csv(output,sep='|')
        df_req = df_req.loc[:, ~df_req.columns.duplicated()]

    else:
        df_req = pd.DataFrame()
    
    # Liste des fichiers Excel dans le répertoire d'entrée
    #fichiers_excel = [f for f in os.listdir(input_file_path) if f.endswith(".xlsx") and f.startswith("DDECLIGOS")]
    fichiers_excel = [f for f in os.listdir(input_file_path) if f.endswith(".xlsx") and "DDECLIGOS" in f and "SWAN" in f]

    if fichiers_excel:
        # Trier les fichiers par date de modification (le plus récent en premier)
        fichiers_excel.sort(key=lambda x: os.path.getmtime(os.path.join(input_file_path, x)), reverse=True)
        fichier_plus_recent = fichiers_excel[0]
        chemin_fichier_entree = os.path.join(input_file_path, fichier_plus_recent)

        # Charger la feuille spécifiée du fichier Excel dans un DataFrame 
        df_extrait = pd.read_excel(chemin_fichier_entree, sheet_name='Opérations par EDS', header=1 ,engine='openpyxl')
        df_extrait = df_extrait.drop(columns=df_extrait.columns[0])
        df_extrait = df_extrait.dropna(how='all')

        for i, row in df_extrait.iterrows():
            # Si la colonne 'Nom court EDS pilote' (Nom court EDS pilote) contient une nouvelle valeur (pas NaN)
            if pd.notna(row['Nom court EDS pilote']) and row['Nom court EDS pilote'] != 'Nom court EDS pilote':
                dernier_nom_court = row['Nom court EDS pilote']
        
            # Remplir les NaN dans la colonne 'Nom court EDS pilote' avec la dernière valeur non-NaN
            elif pd.isna(row['Nom court EDS pilote']):
                df_extrait.at[i, 'Nom court EDS pilote'] = dernier_nom_court

        # Convertir la colonne 'Date création (UTC)' en type datetime, avec des erreurs converties en NaT (Not a Time)
        df_extrait['Date création (UTC)'] = pd.to_datetime(df_extrait['Date création (UTC)'], errors='coerce')
        # Supprimer les lignes où 'Date création (UTC)' est NaT (c'est-à-dire où la conversion en date a échoué)
        df_extrait = df_extrait.dropna(subset=['Date création (UTC)'])
        df_extrait = df_extrait[df_extrait['Date création (UTC)'].dt.year >= 2024]

        colonnes_a_conserver = ['N° opération','Libellé état organisation','Description opération','Date création (UTC)','Date début', 'Nom court EDS pilote', 
                                'Date état Préparé (UTC)', 'Date dernier état Pris en charge(UTC)', 'Date état Validé (UTC)', 'Date fin initiale (UTC)',
                                'Date fin validée (UTC)', 'Date fin révisée (UTC)', 'Date état Terminé (UTC)', 'Date fin réelle (UTC)', 'Nom tech. pilote', 
                                'Nom tech. valid.', 'Nom tech. dem.', 'Nom tech. resp.', 'Opération traitée hors delai', 'Nombre operations traitées hors delais']
        df_extrait = df_extrait[colonnes_a_conserver]

        # Calcul de la date de fin reelle de fermerture du ticket
        df_extrait['Date_de_Fin'] = df_extrait['Date fin réelle (UTC)'].fillna(df_extrait['Date fin révisée (UTC)']).fillna(df_extrait['Date fin initiale (UTC)'])

        print(df_extrait.head())
        print(df_extrait.columns)
        print("Lignes dans df_extrait:", df_extrait.shape[0], "Colonnes dans df_extrait:", df_extrait.shape[1])

    # Suppression des anciens fichiers source DDECLIGOS


    if df_req.empty:
        df_req = df_extrait.copy()
    else:
        for _, row in df_extrait.iterrows():
            numero_op = row['N° opération']
            if numero_op in df_req['N° opération'].values:
                indice_ligne = df_req.index[df_req['N° opération'] == numero_op].tolist()[0]
                # Mettre à jour la ligne existante dans df_req avec les valeurs de row
                df_req.iloc[indice_ligne] = row
            else:
                # Concaténer la ligne de df_extrait à df_req
                df_req = pd.concat([df_req, row.to_frame().T], ignore_index=True)


    if df_req is not None:
    # Enregistrer le DataFrame df_req mis à jour

        df_req.to_csv(output, index=False, sep='|', encoding='UTF-8')
        df_req.to_excel('Requetes.xlsx', index=False, engine='openpyxl')
        end_time = time.time()
        execution_time_seconds = end_time - start_time
        execution_time_minutes = execution_time_seconds / 60
        print("-------------------------------------------------------------------------")
        print("Le fichier de DDECLIGOS le plus récent :", fichier_plus_recent)
        print("Nombre de lignes de Requetes :", df_req.shape[0], "Nombre de colonnes de Requetes:", df_req.shape[1])
        print(f"Extraction, traitement et mise à jour terminés faits en {execution_time_minutes} Minutes le {datetime.today()}.")
    else:
        print("Le chargement du fichier CSV existant a échoué.")


# Exemple d'utilisation de la fonction
input_file_path = 'Fichiers_source' 
output = 'Requetes.csv'

extract_and_update_req(input_file_path, output)