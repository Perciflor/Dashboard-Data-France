import requests
from multiprocessing import Pool
from bs4 import BeautifulSoup as bs 
import pandas as pd 
from pandas import DataFrame
import csv
from pprint import pprint
import json
import os
import time
import re

colonnes = ['ville','lien', 'Nathalie LOISEAU', 'Yannick JADOT', 'François-Xavier BELLAMY', 'Raphaël GLUCKSMANN', 'Jordan BARDELLA', 
'Manon AUBRY', 'Benoît HAMON', 'Ian BROSSAT', 'Jean-Christophe LAGARDE', 'Dominique BOURG', 'Hélène THOUY', 
'Nicolas DUPONT-AIGNAN', 'François ASSELINEAU', 'Florie MARIE', 'Nathalie ARTHAUD', 'Florian PHILIPPOT', 'Francis LALANNE', 
'Nagib AZERGUI', 'Sophie CAILLAUD', 'Nathalie TOMASINI', 'Olivier BIDOU', 'Yves GERNIGON', 'Pierre DIEUMEGARD', 
'Christian Luc PERSON', 'Thérèse DELFEL', 'Audric ALEXANDRE', 'Hamada TRAORÉ', 'Robert DE PREVOISIN', 'Vincent VAUCLIN', 
'Gilles HELGEN', 'Antonio SANCHEZ', 'Renaud CAMUS', 'Christophe CHALENÇON', 'Cathy Denise Ginette CORBET',
"Taux de participation", "Taux d'abstention", "Votes blancs (en pourcentage des votes exprimés)",
"Votes nuls (en pourcentage des votes exprimés)", "Nombre de votants"]

# fonction qui va faire la difference entre les liens scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

if os.path.isfile('dataset\\elections.csv'):
    tableauElections = pd.read_csv('dataset\\elections.csv', error_bad_lines=False, dtype='unicode')
    colonnes1 = tableauElections['lien']
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    # Creation de notre csv infos
    tableauElections = DataFrame(columns= colonnes)
    tableauElections.to_csv('dataset\\elections.csv', index=False)
    # Je recupere la liste des liens a scraper
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']
# print(listeLiens)


def parse(lien):
    dico={i : '' for i in colonnes}
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]
    req = requests.get("https://election-europeenne.linternaute.com/resultats/" + lien[18:])
    time.sleep(2)
    if req.status_code == 200:
        with open('dataset\\elections.csv', 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            divs = soup.findAll('div', class_ = "marB20")

            tableau = divs[3]

            candidats = tableau.findAll('tr', class_ = re.compile('color'))
            for candidat in candidats:
                cle = candidat.find('strong').text
                valeur = candidat.findAll('td')[1].text.replace(',','.').replace('%','')
                try:
                    dico[cle] = float(valeur)
                except:
                    dico[cle] = valeur

            tables = tableau.findAll('table')
            if len(tables) == 2:
                for info in tables[1].findAll('tr')[1:]:
                    cle = info.findAll('td')[0].text
                    valeur = info.findAll('td')[1].text.replace(',','.').replace('%','').replace(' ','')
                    try:
                        dico[cle] = float(valeur)
                    except:
                        dico[cle] = valeur

            writer.writerow(dico)
            print("[elections]", lien)

if __name__ == "__main__":
    with Pool(30) as p:
        p.map(parse, listeLiens)