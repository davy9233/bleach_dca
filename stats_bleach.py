import pymongo
import re
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import statistics
import pandas as pd

client=MongoClient('localhost',27017)
db=client.bleach


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

for k in dico_col:
    print(k)





