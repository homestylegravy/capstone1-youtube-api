import requests
from collections import OrderedDict
import pandas as pd

api_key = '''AIzaSyB2h_8Ck8zAIB5iDEgiOopqUL3VrR3yLX'''
#plus ten minus 6


categories_url = 'https://www.googleapis.com/youtube/v3/videoCategories?part=snippet&regionCode=US&key='
guide_categories_url = 'https://www.googleapis.com/youtube/v3/guideCategories?part=snippet&regionCode=US&key='

r_cat  = requests.request('GET', categories_url + api_key)
r_gcat = requests.request('GET', guide_categories_url + api_key)

def drill_down(nest, level=''):
    '''prints the structure out of a json object'''
    for n, a in enumerate(nest):        
        t = type(nest[a])
        print(level, n, a, type(nest[a]), ('of '+ str(type(nest[a][0])) if t == list else '') )
        if type(nest[a]) == list:
            drill_down(nest[a][0], level=level+'\t')
        elif type(nest[a]) == dict:
            drill_down(nest[a], level = level + '\t')

drill_down( r_cat.json())
drill_down(r_gcat.json())

#put the categories into a pandas df
j = r_cat.json()
df = pd.DataFrame(j['items'])
categories =       df.join(df.from_records(df['snippet']))

j = r_gcat.json()
df = pd.DataFrame(j['items'])
guide_categories = df.join(df.from_records(df['snippet']))


df7 = categories.loc[:,['id', 'title', 'assignable']]
df7.sort_values(['title'])

df8 = guide_categories[['id', 'title']]
df8.sort_values(['title'])
