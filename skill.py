from bs4 import BeautifulSoup
import requests
from pymongo import MongoClient
from perso import personnages
import re 
import random

# recuperation des points de stats
def stats(nom,url):
    page=requests.get(url)
    soup=BeautifulSoup(page.content,'html.parser')
    liste_stat=[]
    for k in soup.find_all('td'):
        a=k.text
        a=re.sub(" |\n","",a)
        if len(a) in (3,4):
            liste_stat.append(a)
    dico_s={"nom":nom,"Stamina": int(liste_stat[0]),"Attack":int(liste_stat[1]),"Defense":int(liste_stat[2]),"Focus":int(liste_stat[3]),"SpiritualPressure":int(liste_stat[4]) }
    return dico_s

#recuperation des skill de perso
def skill_perso(nom,url):
    dicoskill={}
    page=requests.get(url)
    soup = BeautifulSoup(page.content,'html.parser')
    dicoskill["nom"]=nom
    for k in soup.find_all('li') :
        for l in k.find_all('b') :
            a=re.sub("\n","",k.text)
            a=a.split(":")
            if re.search("Evolves from 5|Resurrects into 6",a[0]) == None :
                if len(a) !=1 :
                    dicoskill[a[0]] = a[1] 
                else :
                    dicoskill[a[0]] = a[0]
    return dicoskill

def attrib_perso(nom,url):
    dico={}
    dico["nom"]= nom
    liste_c=[]
    liste_c2=[]
    liste_c3=[]
    page=requests.get(url)
    soup = BeautifulSoup(page.content,'html.parser')
    for k in soup.find_all('div', class_ = "pi-item pi-data pi-item-spacing pi-border-color"):
        a=k.find('h3').text
        liste_c=[]
        for l in k.find_all('div', class_ ="pi-data-value pi-font"):
            liste_c.append(l.text)
            liste_c2=[]
            for m in l.find_all('a'):
                liste_c2.append(m.get('href'))
            liste_c3=[]
            for n in l.find_all('li'):
                liste_c3.append(n.text)
            if len(liste_c)<len(liste_c2):
                liste_final='|'.join(liste_c2)
            else :
                if len(liste_c)<len(liste_c3):
                    liste_final='|'.join(liste_c3)
                else :
                    liste_final='|'.join(liste_c)
            liste_final=re.sub("\/wiki\/Category\:Affiliation\:\_","",liste_final) 
            dico[a]=liste_final
    return dico


# recuperation de la liste des perso et lien
page = requests.get('https://bleach-bravesouls.fandom.com/wiki/Characters')

soup = BeautifulSoup(page.content,'html.parser')

liste_perso=[]
dico_perso={}
for k in soup.find_all('th'):
    for l in k.find_all('a'):
        dico_perso={'nom':(l.text),'lien':(f"https://bleach-bravesouls.fandom.com{l.get('href')}")}
        liste_perso.append(dico_perso)

print("recup des persos ")

#recuperation des perso avec tous leurs types et lien
liste_final=[]
dico_final={}

for k in liste_perso:
    page = requests.get(k["lien"])
    soup = BeautifulSoup(page.content,'html.parser')
    for k in soup.find_all('th'):
        for l in k.find_all('a'):
            if re.search("images",l.get('href'))== None:
                if re.search("6\%",l.get('href')):
                    a=re.sub("\/wiki\/6\%E2\%98\%85\_","",(l.get('href')))
                    dico_final={'nom' : a, 'lien':(f"https://bleach-bravesouls.fandom.com{l.get('href')}")}
                    liste_final.append(dico_final)
                    print(dico_final)

print("recup des persos avec leur variantes")

#integration des perso dans la base nosql
client=MongoClient('localhost',27017)
db=client.bleach
db.perso.insert_many(liste_final)


stats_final=[]
for k in liste_final:
    dico_stats = stats(k["nom"],k["lien"])
    stats_final.append(dico_stats)

db.stats_perso.insert_many(stats_final)

print("recup de stats des persos ")

attrib_final=[]
for k in liste_final:
    dico_attrib = attrib_perso(k["nom"],k["lien"])
    attrib_final.append(dico_attrib)

db.attrib_perso.insert_many(attrib_final)


print("recup de attributs des persos")

skill_final=[]
for k in liste_final:
    dico_skill = skill_perso(k["nom"],k["lien"])
    skill_final.append(dico_skill)


db.skill_perso.insert_many(skill_final)

print("creation de dataframe")

#creation de dataframes
import pandas as pd
import pickle
from datetime import time,date,datetime
from os import path
import os

sd=datetime.now()
path_d="C:/perso/bleach/sav/"


dico_col={}
liste_col=[]
for k in db.list_collection_names():
    m=k+"_df"
    dico_col[k]=m
    print(f"k : {m}")
    command = "db."+k+".find()"
    collection_cursor = eval(command)
    dico_col[k] = pd.DataFrame(list(collection_cursor))
    print (dico_col[k])
    file_p="C:/perso/bleach/sav/bleach_"+k+".pkl"
    if path.exists(file_p):
        filename =sd.strftime("%H%M%S%d%m")+k+".pkl"
        dest= path_d+filename
        os.rename(file_p,dest)
        print('trouve')
    dico_col[k].to_pickle(f'''./sav/bleach_{k}.pkl''')
