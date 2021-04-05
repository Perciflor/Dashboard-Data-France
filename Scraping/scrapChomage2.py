from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait as wait
from bs4 import BeautifulSoup as bs 
from pprint import pprint
import pandas as pd 
from pandas import DataFrame
import csv
import os
import time

# Initialisation d'un dico
dico = {
    **{i : '' for i in ['ville','lien']},
    **{str(annee) : '' for annee in range(2003,2020)}
}

colonnes = list(dico.keys())

# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

# Parer a l'eventualite que le script s'est arreter
if os.path.isfile("dataset\\chomage.csv"):
    tableauChomage = pd.read_csv('dataset\\chomage.csv', error_bad_lines=False, dtype='unicode')
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    colonnes1 = tableauChomage['lien']
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    tableauChomage = DataFrame(columns = colonnes)
    tableauChomage.to_csv('dataset\\chomage.csv', index=False)

    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']

# On ouvre un navigateur
navigateur = webdriver.Firefox(executable_path="geckodriver.exe")
navigateur.maximize_window()


compteur = 0
for lien in listeLiens:
    with open('dataset\\chomage.csv', 'a', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator= '\n')
        ville = lien.split('/')[3]
        departement = lien.split('/')[-1][-5:-3]
        code_insee = lien.split('/')[-1][-5:]

        # Initialisation d'un dico
        dico = {
            **{i : '' for i in ['ville','lien']},
            **{str(annee) : '' for annee in range(2003,2020)}
        }
        dico['lien'] = lien
        dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

        try:
            # on se connecte au lien
            navigateur.get("https://ville-data.com/chomage/" + '-'.join([ville,departement,code_insee]))

            # on scroll
            navigateur.execute_script("window.scrollTo(0, 1000);")
            time.sleep(1.5)
            soup = bs(navigateur.page_source, "html.parser")
            div = soup.find(id='tauxchomage')
            table = div.find('table')
    
            tousLesTr = table.findAll('tr')
            for tr in tousLesTr[1:]:
                cle = tr.findAll('td')[0].text
                valeur = tr.findAll('td')[1].text
                dico[cle] = valeur
            writer.writerow(dico)
        except:
            dico = {str(annee) : '' for annee in range(2003,2020)}
            dico['lien'] = lien
            dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]
            writer.writerow(dico)            

        compteur = compteur + 1
        print(compteur)



