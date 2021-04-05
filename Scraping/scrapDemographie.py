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

listeCles = ["ville","lien","Population","Densité de population","Nombre de ménages","Habitants par ménage",
"Nombre de familles","Naissances","Décès","Solde naturel","Hommes","Femmes","Moins de 15 ans","15 - 29 ans","30 - 44 ans",
"45 - 59 ans","60 - 74 ans","75 ans et plus","Familles monoparentales","Couples sans enfant","Couples avec enfant",
"Familles sans enfant","Familles avec un enfant","Familles avec deux enfants","Familles avec trois enfants",
"Familles avec quatre enfants ou plus","Personnes célibataires","Personnes en concubinage", "Personnes pacsées","Personnes mariées","Personnes divorcées","Personnes veuves","Population étrangère","Hommes étrangers","Femmes étrangères","Moins de 15 ans étrangers","15-24 ans étrangers","25-54 ans étrangers","55 ans et plus étrangers","Population immigrée","Hommes immigrés","Femmes immigrées","Moins de 15 ans immigrés","15-24 ans immigrés","25-54 ans immigrés","55 ans et plus immigrés"]

dico = {
    **{i : '' for i in listeCles},
    **{"nbre habitants (" + str(a) + ")" : '' for a in range(2006,2019)},
    **{"nbre naissances (" + str(a) + ")" : '' for a in range(1999,2020)},
    **{"nbre deces (" + str(a) + ")" : '' for a in range(1999,2020)},
    **{"nbre étrangers (" + str(a) + ")" : '' for a in range(2006,2018)},
    **{"nbre immigrés (" + str(a) + ")" : '' for a in range(2006,2018)},
    }


colonnes = list(dico.keys())

# fonction qui va faire la difference entre les liens scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

if os.path.isfile('dataset\\demographie.csv'):
    tableauDemo = pd.read_csv('dataset\\demographie.csv', error_bad_lines=False, dtype='unicode')
    colonnes1 = tableauDemo['lien']
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    # Creation de notre csv infos
    tableauDemo = DataFrame(columns= colonnes)
    tableauDemo.to_csv('dataset\\demographie.csv', index=False)
    # Je recupere la liste des liens a scraper
    tableauLiens = pd.read_csv('dataset\\liensVilles.csv')
    listeLiens = tableauLiens['lien']

listeLiens = [lien for lien in listeLiens if lien[:11] == '/management']

def parse(lien):
    # Initialisation d'un dictionnaire
    dico = {
        **{i : '' for i in listeCles},
        **{"nbre habitants (" + str(a) + ")" : '' for a in range(2006,2019)},
        **{"nbre naissances (" + str(a) + ")" : '' for a in range(1999,2020)},
        **{"nbre deces (" + str(a) + ")" : '' for a in range(1999,2020)},
        **{"nbre étrangers (" + str(a) + ")" : '' for a in range(2006,2018)},
        **{"nbre immigrés (" + str(a) + ")" : '' for a in range(2006,2018)},
        }


    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

    req = requests.get("http://www.journaldunet.com" + lien + "/demographie")
    time.sleep(2)
    if req.status_code == 200:
        with open('dataset\\demographie.csv', 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            tables = soup.findAll('table', class_= "odTable odTableAuto")

            for i in range(len(tables)):
                infos = tables[i].findAll('tr')
                for info in infos[1:]:
                    cle = info.findAll('td')[0].text
                    valeur = info.findAll('td')[1].text

                    cle = cle.split('(')[0].strip()
                    valeur = valeur.split('h')[0].strip().replace(',', '.')
                    try:
                        dico[cle] = float(valeur)
                    except:
                        dico[cle] = valeur

            divs = soup.findAll('div', class_="hidden marB20")
            for div in divs:
                titre_h2 = div.find('h2')
                if titre_h2 != None and "Nombre d'habitants" in titre_h2.text:
                    if div.find('script').string:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        donnees = json_data['series'][0]['data']

                        for annee, donnee in zip(annees, donnees):
                            try:
                                dico["nbre habitants (" + str(annee) + ")"] = float(donnee)
                            except:
                                dico["nbre habitants (" + str(annee) + ")"] = ''

                if titre_h2 != None and "Naissances et décès" in titre_h2.text:
                    if div.find('script').string:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        if len(json_data['series']) != 0:
                            naissances = json_data['series'][0]['data']
                            deces = json_data['series'][1]['data']
                            for annee, n, d in zip(annees, naissances, deces):
                                try:
                                    dico["nbre naissances (" + str(annee) + ")"] = float(n)
                                    dico["nbre deces (" + str(annee) + ")"] = float(d)
                                except:
                                    dico["nbre naissances (" + str(annee) + ")"] = ''
                                    dico["nbre deces (" + str(annee) + ")"] = ''
                        else:
                            dico["nbre naissances (" + str(annee) + ")"] = ''
                            dico["nbre deces (" + str(annee) + ")"] = ''

                if titre_h2 != None and "Nombre d'étrangers" in titre_h2.text:
                    if div.find('script').string:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        etrangers = json_data['series'][0]['data']

                        for annee, etranger in zip(annees, etrangers):
                            try:
                                dico["nbre étrangers (" + str(annee) + ")"] = float(etranger)
                            except:
                                dico["nbre étrangers (" + str(annee) + ")"] = ''

                if titre_h2 != None and "Nombre d'immigrés" in titre_h2.text:
                    if div.find('script').string:
                        js_script = div.find('script').string
                        json_data = json.loads(js_script)
                        annees = json_data['xAxis']['categories']
                        immigres = json_data['series'][0]['data']

                        for annee, immigre in zip(annees, immigres):
                            try:
                                dico["nbre immigrés (" + str(annee) + ")"] = float(immigre)
                            except:
                                dico["nbre immigrés (" + str(annee) + ")"] = ''

            writer.writerow(dico)
            print("[demographie]",lien)

if __name__ == "__main__":
    with Pool(30) as p:
        p.map(parse, listeLiens)