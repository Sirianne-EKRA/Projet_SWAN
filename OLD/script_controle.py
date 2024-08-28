import os
import pandas as pd
import openpyxl
import time
from datetime import datetime, timedelta

def extract_and_update_controle(input_file_path, output, plateformes_path):
    start_time = time.time()
    # Charger le fichier CSV existant

    if  os.path.exists(output):
        df_controle = pd.read_csv(output,sep='|')
        df_controle = df_controle.loc[:, ~df_controle.columns.duplicated()]

    else:
        df_controle = pd.DataFrame()
        
    # Liste des fichiers Excel dans le répertoire d'entrée
    fichiers_excel = [f for f in os.listdir(input_file_path) if f.endswith(".xlsx") and f.startswith("Contrôle")]
    df_plateformes = pd.read_excel(plateformes_path, engine='openpyxl')
    fichiers_reactivity = [f for f in os.listdir(input_file_path) if f.endswith(".xlsx") and f.startswith("FISRT REACTIVITY")]

    dict_service = {'GRAFANA': ['OPGRAF'],
    'ISOS': ['OPISOS'],
    'Orange Money': ['OP0MC'], 
    'KYC': ['KYC'],
    'MAXIT': ['MAXIT'],
    'MESSAGING PRO': ['OPBUS', 'BULK'],
    'ESX': ['ESX'],
    'ONE FRA': ['OPFRA', 'FRAS'],
    'OBA': ['OPOBA'],
    'NOMAD': ['OPNS'],
    'WAAAT': ['W3A'],
    'ORANGE INFRA': ['NANO'],
    'ENV TECHNIQUE': ['CELLULE','TGBT','DCBT'],
    'GTW mutualisée': ['OPGMI'],
    'GTW Orange Money': ['PRN-','OPC3'],
    'IVRaaS': ['OPIVR'],
    'MyOrange': ['OMYO'],
    'AXWAY': ['OPGOSVTBAXWAY','AXWAY'],
    'SDP': ['opsdp','OPSDP','SDP'],
    'MVA': ['OPMVA'],
    'USSD SHOP': ['OPUSD'],
    'KMC': ['KMC'],
    'OMS': ['OMS']}
    dict_country = {
       'OCF':['OICF','CF','OCF'],'OCI':['OICI','CI','OCI'],'VITIB':['VTB'],'OMG':['MG','OMG'], 'OML':['OML'], 'OSN':['OSN'], 'OCD':['OCD'], 'OCM':['OCM','CM'], 'OJO':['OJO'], 'OGN':['OGN'], 'OGW':['OGW'], 'OBW':['OBW'],'OBF':['OBF','BF'],'OSL':['SL','OSL']}

    if fichiers_excel:
        # Trier les fichiers par date de modification (le plus récent en premier)
        fichiers_excel.sort(key=lambda x: os.path.getmtime(os.path.join(input_file_path, x)), reverse=True)
         # Sélectionner le fichier Excel le plus récent
        fichier_plus_recent = fichiers_excel[0]

        # Chemin complet du fichier Excel le plus récent
        chemin_fichier_entree = os.path.join(input_file_path, fichier_plus_recent)

        # Charger la feuille spécifiée du fichier Excel dans un DataFrame pandas
        df_extrait = pd.read_excel(chemin_fichier_entree, sheet_name='Liste des Tickets', skiprows=7, engine='openpyxl')
        df_extrait = df_extrait.dropna(how='all')
        df_extrait = df_extrait.iloc[:, 3:]
        df_extrait = df_extrait.loc[:, ~df_extrait.columns.duplicated()]
        df_extrait = df_extrait.drop(['Description', 'Cause label', 'Short label', 'temps_rétablissement'], axis=1)
        df_extrait = df_extrait.drop(df_extrait.columns[[10, 11]], axis=1)
        df_extrait = df_extrait.rename(columns={'contry\n': 'Country', 'Plateformes\n': 'Plateformes', 'Services\n': 'Services', 'Cause\n': 'Cause'})
        df_extrait['Country'] = df_extrait['Country'].astype(str)

        # Supprimer tous les autres fichiers "Controle"
        for fichier in fichiers_excel[1:]:
            chemin_fichier = os.path.join(input_file_path, fichier)
            os.remove(chemin_fichier)

    if fichiers_reactivity:
        fichiers_reactivity.sort(key=lambda x: os.path.getmtime(os.path.join(input_file_path, x)), reverse=True)
         # Sélectionner le fichier Excel le plus récent
        fichier_plus_recent_reactivity = fichiers_reactivity[0]
        chemin_fichier_reactivity = os.path.join(input_file_path, fichier_plus_recent_reactivity)
        df_reactivity = pd.read_excel(chemin_fichier_reactivity, sheet_name=4, engine='openpyxl')
        # Supprimer tous les autres fichiers "FIRST REACTIVITY"
        #for fichier in fichiers_reactivity[1:]:
            #chemin_fichier_reactivity = os.path.join(input_file_path, fichier)
            #os.remove(chemin_fichier_reactivity)


    if df_controle.empty:
        df_controle = df_extrait.copy()
    else:
        # Parcourir les lignes de df_extrait
        for _, row in df_extrait.iterrows():
            ticket_id = row['Ticket ID']
            # Vérifier si le Ticket ID existe déjà dans df_controle
            if ticket_id in df_controle['Ticket ID'].values:
                # Récupérer l'indice de la ligne correspondante dans df_controle
                indice_ligne_combine = df_controle.index[df_controle['Ticket ID'] == ticket_id].tolist()[0]
                # Mettre à jour la ligne existante dans df_controle avec les valeurs de row
                df_controle.iloc[indice_ligne_combine] = row
            else:
                # Concaténer la ligne de df_extrait à df_controle
                df_controle = pd.concat([df_controle, row.to_frame().T], ignore_index=True)

    # Parcourir les lignes du premier DataFrame
    for index1, row1 in df_controle.iterrows():
        plateforme1 = row1['Plateformes']
        pays1 = row1['Country']
        services1 = row1['Services']

        # Parcourir les lignes du deuxième DataFrame
        for index2, row2 in df_plateformes.iterrows():
            plateforme2 = row2['Plateformes']
            pays2 = row2['Country_serv']
            services2 = row2['services_plat']

            # Comparaison des plateformes en tenant compte des valeurs vides
            if plateforme1 == plateforme2 :
                df_controle.at[index1, 'Country'] = pays2
                df_controle.at[index1, 'Services'] = services2

        # Vérifier si la colonne 'Country' est vide ou NaN
        if pd.isna(row1['Country']) or row1['Country'] == '':
            # Mettre à jour la colonne 'Services' en fonction du dictionnaire de mappage des services
            for value_service, keys_service in dict_service.items():
                if any(key in str(row1['Plateformes']) for key in keys_service):
                    df_controle.at[index1, 'Services'] = value_service

            # Mettre à jour la colonne 'Country' en fonction du dictionnaire de mappage des pays
            for value_country, keys_country in dict_country.items():
                if any(key in str(row1['Plateformes']) for key in keys_country):
                    df_controle.at[index1, 'Country'] = value_country

    # Ajout des données de délai de réponse
    # Suppression des doublons en gardant la ligne avec la valeur minimale dans '1st reactivity (min.)'
    df_reactivity = df_reactivity.sort_values(by='1st reactivity (min.)').drop_duplicates(subset=['Ticket ID'], keep='first')
    df_controle = pd.merge(df_controle, df_reactivity[['Ticket ID', 'Follow-up start date', 'Handling date', '1st reactivity (min.)']], on='Ticket ID', how='left')


    # Vérifier et ajouter les nouvelles plateformes dans df_plateformes
    for index1, row1 in df_controle.iterrows():
        plateforme1 = row1['Plateformes']
        pays1 = row1['Country']
        services1 = row1['Services']
        
        # Si la plateforme n'existe pas dans df_plateformes, l'ajouter
        if not any(df_plateformes['Plateformes'] == plateforme1):
            new_row = {'Plateformes': plateforme1, 'Country_serv': pays1, 'services_plat': services1, 'SITE':"NEW"}
            df_plateformes = pd.concat([df_plateformes, pd.DataFrame([new_row])], ignore_index=True)


    # Écrire le fichier CSV final
    df_controle['Initiator - User name'] = df_controle['Initiator - User name'].str.upper()
    df_controle['Restoration user name'] = df_controle['Restoration user name'].str.upper()

    df_controle.to_csv(output, index=False, sep='|', encoding='UTF-8')

    # Enregistrer le DataFrame combiné et le DataFrame plateformes mis à jour
    if df_controle is not None:
        df_plateformes = df_plateformes.drop_duplicates()
        df_controle.to_csv(output, index=False, sep='|', encoding='UTF-8')
        df_plateformes.to_excel(plateformes_path, index=False, engine='openpyxl')
        #df_plateformes.to_csv('/home/ajunior/Brise_Project/Final_Data/plateformes_base.csv', index=False, sep='|', encoding='UTF-8')
        end_time = time.time()
        execution_time_seconds = end_time - start_time
        execution_time_minutes = execution_time_seconds / 60
        print("-------------------------------------------------------------------------")
        print("Le fichier de Contrôle le plus récent :", fichier_plus_recent)
        print("Nombre de lignes de Controle_data:", df_controle.shape[0], "Nombre de colonnes:", df_controle.shape[1])
        print(f"Extraction, traitement et mise à jour terminés faits en {execution_time_minutes} Minutes le {datetime.today()}.")
    else:
        print("Le chargement du fichier CSV existant a échoué.")

# Exemple d'utilisation de la fonction
input_file_path = '/home/ajunior/' 
output = '/home/ajunior/Brise_Project/Temporary_Data/Controle_data.csv'
plateformes_path = '/home/ajunior/Brise_Project/Final_Data/plateformes_base.xlsx'

extract_and_update_controle(input_file_path, output, plateformes_path)
