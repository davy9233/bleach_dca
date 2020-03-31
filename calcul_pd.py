import pandas as  pd 
import pickle
import re
import os


attrib_perso=pd.read_pickle("C:/perso/bleach/sav/bleach_attrib_perso.pkl")
perso=pd.read_pickle("C:/perso/bleach/sav/bleach_perso.pkl")
skill_perso=pd.read_pickle("C:/perso/bleach/sav/bleach_skill_perso.pkl")
stats_perso=pd.read_pickle("C:/perso/bleach/sav/bleach_stats_perso.pkl")


attrib_perso.set_index('nom')
perso.set_index('nom')
skill_perso.set_index('nom')
stats_perso.set_index('nom')


final=attrib_perso.merge(skill_perso,on='nom')
final=final.merge(stats_perso,on='nom')
final=final.merge(perso,on='nom')

