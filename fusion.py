import os
import pandas as pd
import time
from datetime import datetime, timedelta

def merge(changement_CAB_file_path, requete_file_path, output):

    # Charger les fichiers CSV
    start_time = time.time()

    # Charger les fichiers CSV
    df_cab = pd.read_csv(changement_CAB_file_path, sep='|')
    df_req = pd.read_csv(requete_file_path, sep='|')

    colonnes_a_conserver_cab = ['N° opération','Libellé état organisation','Description opération','Date création','Date début', 
                                'Date fin initiale', 'Date fin révisée', 'Date fin réelle', 'Date_de_Fin',
                                'Nom tech. pilote', 'Nom court EDS pilote', 'Nom tech. valid.', 'Nom tech. dem.', 'Nom tech. resp.',
                                'Nombre operations traitées hors delais', 'Libellé modèle opération']
    
    colonnes_a_conserver_req = ['N° opération','Libellé état organisation','Description opération','Date création (UTC)','Date début', 
                                'Date fin initiale (UTC)', 'Date fin révisée (UTC)', 'Date fin réelle (UTC)', 
                                'Date_de_Fin', 'Nom tech. pilote', 'Nom court EDS pilote', 'Nom tech. valid.', 'Nom tech. dem.', 'Nom tech. resp.',
                                'Nombre operations traitées hors delais']
    
    df_req = df_req[colonnes_a_conserver_req] 
    df_cab = df_cab[colonnes_a_conserver_cab]


    # Renommer les colonnes
    df_req.rename(columns={
        'Date création (UTC)': 'Date création',
        'Date fin révisée (UTC)': 'Date fin révisée',
        'Date fin réelle (UTC)': 'Date fin réelle',
        'Date fin initiale (UTC)': 'Date fin initiale'
    }, inplace=True)

    df_req['Libellé modèle opération'] = ''

    # Réorganiser les colonnes dans l'ordre spécifié
    columns_order = ['N° opération','Libellé état organisation','Description opération','Date création','Date début', 
                        'Date fin initiale', 'Date fin révisée', 'Date fin validée',  'Date fin réelle', 'Date_de_Fin',
                        'Nom tech. pilote', 'Nom court EDS pilote', 'Nom tech. valid.', 'Nom tech. dem.', 'Nom tech. resp.',
                        'Nombre operations traitées hors delais', 'Libellé modèle opération']
    df_req = df_req[columns_order]
    df_cab = df_cab[columns_order]
    
    # Concaténer les deux DataFrames
    df_fusion = pd.concat([df_cab, df_req], ignore_index=True, sort=False)

    if df_fusion is not None:
        # Sauvegarder le résultat dans un nouveau fichier CSV
        df_fusion = df_fusion.drop_duplicates()
        df_fusion.to_csv(output, index=False, sep='|', encoding='UTF-8')
        df_fusion.to_csv(output_hdfs, index=False, sep='|', encoding='UTF-8')
        #df_fusion.to_excel('Swan.xlsx', index=False, engine='openpyxl')

        end_time = time.time()
        execution_time_seconds = end_time - start_time
        execution_time_minutes = execution_time_seconds / 60

        print("Fusion réalisée avec succès. Nombre de lignes:", df_fusion.shape[0], "Nombre de colonnes:", df_fusion.shape[1])
        print(f"Fusion et mise à jour terminées en {execution_time_minutes} Minutes faites le {datetime.today()}.")
    else:
        print("Le chargement des fichiers CSV existants a échoué.")


changement_CAB_file_path = '/home/ajunior/Swan_project/Temporary_data/Changements_CAB.csv'
requete_file_path = '/home/ajunior/Swan_project/Temporary_data/Requetes.csv'
output = '/home/ajunior/Swan_project/Final_data/Swan.csv'
output_hdfs = '/home/ajunior/Swan_project/HDFS_Data/Swan.csv'

merge(changement_CAB_file_path, requete_file_path, output)