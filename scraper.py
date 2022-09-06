import pandas as pd 
import requests
from bs4 import BeautifulSoup 
from anti_useragent import UserAgent
import random


data_url = "https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel/download/?format=csv&timezone=Europe/Berlin&lang=fr&use_labels_for_header=true&csv_separator=%3B"
ua = UserAgent().random

#------------------------------ FONCTIONS ----------------------------------------------

def get_data(url): # Cette fonction prend en entrée une URL et retourne un dataframe avec un pré-traitement.
    
    df = pd.read_csv(url, sep=';')
    df = df.drop(columns=df.columns[-1]) 
    
    # Conversion du timestamp en datetime 
    df['Actualisation de la donnée'] = pd.to_datetime(df['Actualisation de la donnée'],utc=True)
    df['Actualisation de la donnée'] = df['Actualisation de la donnée'].dt.tz_convert(tz='CET')
    df['Actualisation de la donnée'] = df['Actualisation de la donnée'].dt.tz_localize(None)
    
    # Nettoyage des données (filtre sur la date du jour uniquement). Il existe, en effet, des données datant de 2018...
    max_date = df['Actualisation de la donnée'].dt.date.max() 
    df = df[df['Actualisation de la donnée'].dt.date==max_date].sort_values('Actualisation de la donnée',ascending=False)
      
    return df


def get_meteo(villes):  # Prend en entrée une liste de villes et retourne un dataframe avec les villes et la météo correspondante. 
    valeurs = []
    for ville in villes:
        api = f"https://www.prevision-meteo.ch/services/json/{ville}"
        response = requests.get(api, headers = {'headers':ua}, timeout=120)
        try:
            response.json()["current_condition"]
            rows = [ville]
            rows.extend(list(response.json()["current_condition"].values())[2:-3:])
            rows.append("Non")
            valeurs.append(rows)
        except:
            api = f"https://www.prevision-meteo.ch/services/json/paris"
            response = requests.get(api, headers = {'headers':ua}, timeout=120)
            rows = [ville]
            rows.extend(list(response.json()["current_condition"].values())[2:-3:])
            rows.append("Oui")
            valeurs.append(rows)
        
    cols = ["ville"]
    cols.extend(list(response.json()["current_condition"])[2:-3:])
    cols.append("Correction")
    
    return pd.DataFrame(valeurs, columns=cols)

#------------------------------ PROCESS ----------------------------------------------
# 1. Récupération des données Velib
data = get_data(data_url)
# 2. Récupération de la liste des villes 
villes = list(data['Nom communes équipées'].unique())
# 3. Récupération de la météo
meteo_df = get_meteo(villes)
# 4. Jointure des deux dataframe
final_df = pd.merge(left=data, right=meteo_df, how='inner', left_on='Nom communes équipées', right_on='ville')
# 5. Exportation des données au format CSV
final_df.to_csv('data.csv', index=False)

#------------------------------ HISTORISATION ----------------------------------------------
try:
    histo_df = pd.read_csv('histo.csv') 
    historisation = (pd.concat([final_df, histo_df], ignore_index=True, sort =False)
            .drop_duplicates(['Identifiant station','Actualisation de la donnée'], keep='last'))
    historisation.to_csv('histo.csv', index=False)
except:
    final_df.to_csv('histo.csv',index=False)
