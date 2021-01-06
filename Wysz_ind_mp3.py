# -*- coding: utf-8 -*-
"""
Created 9.09.2020
Preprocessing
Indeksowanie i sprawdzenie na gotowo
"""

from elasticsearch import helpers, Elasticsearch
import pandas as pd
import json
import requests
from datetime import datetime

es = Elasticsearch()

fxp = 'C:\\MP\\ES\\Produkty\\'

fxlsx = fxp +'Produkty_mp_ind_es2.xlsx'
fxjson = fxp + 'Produkty_mp_ind_es2.json'
df = pd.read_excel(fxlsx, sheet_name= 'A1')

df = df.applymap(lambda s:s.lower() if type(s) == str else s)
df['NAMEF']=df['NAME'].apply(lambda z: ' '.join(z.split()[0:5]))

def fff(a,b):
    return set(a).difference(b)

df['NAMEF1']=df.apply(lambda z: fff(z['BRAND'], z['NAMEF']), axis=1)

#czyszczenie nazwy
#-----

df['Nazwa1'] = df['NAME'].str.lower()  

print('Wymiana na specję')
df['Nazwa1'] = df['Nazwa1'].replace( r"onnline|Onnline|Online|online|kolor" , " ", regex=True)

df['Nazwa1'] = df['Nazwa1'].replace( r"\b, | ,\b"   , " " , regex=True)
df['Nazwa1'] = df['Nazwa1'].replace( r"\""          , " " , regex=True)
df['Nazwa1'] = df['Nazwa1'].replace( r"(\w)\.(\w)"  , "\\1 \\2" , regex=True)
df['Nazwa1'] = df['Nazwa1'].replace( r"(\w)\.(\s)"    , "\\1 \\2" , regex=True)
#df['Nazwa1'] = df['Nazwa1'].replace( r"(\d)\.\s"    , "\\1 " , regex=True)

df['Nazwa1'] = df['Nazwa1'].replace( r"\(|\)"     , " " , regex=True)

df['Nazwa1'] = df['Nazwa1'].replace( r"-" , " - " , regex=True)

print('1 ',df.Nazwa1[0:1])

print('Wymiana na perern 1 + spacja')
df['Nazwa1'] = df['Nazwa1'].replace( r"([a-zA-ZŁŻŻąćęłóńśżź])(,)", "\\1 ", regex=True)
df['Nazwa1'] = df['Nazwa1'].replace( r"([a-zA-ZŁŻŻąćęłóńśżź])(:)", "\\1 ", regex=True)
print('2 ', df.Nazwa1[0:1])

print('Wymiana Fi, mm, cm itp na bez spacji')

df['Nazwa1'] = df['Nazwa1'].replace( r"(fi|e|ip)([ \-\=])(\d{1,5})(\d)", "\\1\\3 ", regex=True)

df['Nazwa1'] = df['Nazwa1'].replace( r"(\d) (mm |m |cm )",       "\\1\\2", regex=True)
df['Nazwa1'] = df['Nazwa1'].replace( r"(\d)(\\)(/d)",        "\\1/\\2 ", regex=True)
df['Nazwa1'] = df['Nazwa1'].replace( r"(\d)( x )(\d)"  , "\\1x\\2 " , regex=True)



df['Nazwa1'] = df['Nazwa1'].replace( r"\s{2,}"    , " " , regex=True)





df_as_json = df.to_json(fxjson, orient='records')

fcat_xls = fxp + 'Produkty_mp_cat_es2.xlsx'
fcat_json = fxp + 'Produkty_mp_cat_es2.json'
dfc = pd.read_excel(fcat_xls, sheet_name= 'A1')
dfcj = dfc.to_json(fcat_json, orient='records')

#indeksowanie settings-------------------------
# should be:index.analysis.analyzer.default.type: snowball

sett_mp1 = {
    "analysis": {
        "filter": {
            "mp_stop": {
                "type": "stop",
                "stopwords": [ "na", "do", "i", "z" ]
                },
            "mp_syno" : {
                "type" : "synonym",
                "synonyms" : [
                    "xxx1, xxx",
                    "yyy1, yyy"
                    ]
                },
            "mp_syno2" : {
                "type" : "synonym",
                "expand" : False,
                "synonyms" : [
                    "czerpalny => czerpny",
                    "aluminum => amelinium",
                    " x =>x"
                    ]
                },
            "mp_tok_limit": {
                "type": "limit",
                "max_token_count": 20
                }
            },
        "char_filter": {
            "mp_char_filter": {
                "type": "mapping",
                "mappings": [
                    ",=>",
                    ":=>",
                    ")=>",
                    "(=>"
                    ]
                },
            "rgx_char_filter": {
                "pattern": "[^A-Za-z0-9]",
                "type": "pattern_replace",
                "replacement": ""
                }
            },
        "analyzer": {
              "mp_white": { 
                  "type": "custom",
                  "tokenizer": "whitespace",
                  "char_filter": ["html_strip", "mp_char_filter" ],
                  "filter": [
                      "lowercase", 
                      "mp_syno",
                      "mp_syno2",
                      "mp_stop",
                      "mp_tok_limit"
                      ]
                  },
               "mp_autocomplete": {
                   "tokenizer": "autocomplete",
                   "filter": [
                       "lowercase",
                       "mp_stop"
                       ]
                   },
               "autocomplete_search": {
                   "tokenizer": "lowercase",
                   "filter": [
                       "mp_stop"
                       ]
                   }
               },
               "tokenizer": {
                   "autocomplete": {
                       "type": "edge_ngram",
                       "min_gram": 3,
                       "max_gram": 3,
                       "token_chars": [
                           "letter"
                           ]
                       }
                   }
               }
    }
    


'''
BRAND
CA0
CA1
CA2
CA3
CA4
CAT_2LAST
CAT_STR
CAT0
CAT1
CAT2
CAT3
CAT4
DESC
IDX
NAME
PG
PM
PROD_TYP
SAP_NAME
SERIE
SUPP_NAME
SYN
TRANS


'''

mapp_mp5a = {
    "properties":  {
            "BRAND":     { "type": "text", 
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                              },
            "CA0":      { "type": "keyword"},
            "CA1":      { "type": "keyword"},
            "CA2":      { "type": "keyword"},
            "CA3":      { "type": "keyword"},
            "CA4":      { "type": "keyword"},
            "CAT_2LAST": { "type": "text",
                              "analyzer": "mp_autocomplete",
                              "search_analyzer": "autocomplete_search"
                         },
            "CAT_STR":   { "type": "text",          },
            "CAT0":      { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}},
            "CAT1":      { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}},
            "CAT2":      { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}},
            "CAT3":      { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}},
            "CAT4":      { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}},
            "DESC":      { "type": "integer"        },
            "IDX":       { "type": "keyword"     },
            "NAME":      { "type": "text", "analyzer": "mp_white"  }, 
            "PG":        { "type": "text", "analyzer": "mp_white",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                              }, 
            "PM":        { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                              },
            "PROD_TYP":  { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                              },
            "SAP_NAME":  { "type": "text", "analyzer": "mp_white"  },
            "SERIE":     { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                              },
            "SUPP_NAME": { "type": "text"        },
            "SYN":       { "type": "text", "analyzer": "mp_white"  },
            "TRANS":     { "type": "integer"     }
                } 
              } 


es.indices.delete(index='p5')
#Zmiana settings


es.indices.create(index='p5',
    body={
      'settings': sett_mp1,
      'mappings': mapp_mp5a,

      }
    )


#generator to push bulk data from a JSON
def bulk_json_data():
    with open(fxjson,'r') as f:
        json_data = json.load(f) 
        for doc in json_data:
            yield {
#                "_index": "p4",
                "_id": doc['IDX'],
                "_source": doc
                }

helpers.bulk(es, bulk_json_data(), index='p5',)

'''
CAT4
CAT3
CAT2
CAT1
CAT0
CK1
CK2
CK3
CK4
CK5
CAT_DL
CAT_ID

'''

mapp_cat5a = {
    "properties":  {
         "CAT4":     { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                              },
         "CAT3":     { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                              },
         "CAT2":     { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                              },
         "CAT1":     { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                              },
         "CAT0":     { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                              },
         "CK1":     { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                              },
         "CK2":     { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                              },
         "CK3":     { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                              },
         "CK4":     { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                              },
         "CK5":     { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                              },
         "CAT_DL":     { "type": "integer"   },
         "CAT_ID":     { "type": "text",
                           "fields":{"keyword": {
                                 "type": "keyword",
                                 "ignore_above": 128
                                 }}
                           }
         }
   } 


es.indices.delete(index='c5')


es.indices.create(index='c5',
    body={
      'settings': sett_mp1,
      'mappings': mapp_cat5a,

      }
    )


def load_cat_json():
    with open(fcat_json,'r') as fc:
        dfcj_data = json.load(fc) 
        for doc in dfcj_data:
            yield {
                "_id": doc['CAT0'],
                "_source": doc
                }

helpers.bulk(es, load_cat_json(), index='c5',)



cat15flat_fname = "Prod_cat_15vs_flat.xlsx"
fcat_xls2 = fxp + cat15flat_fname
fcat_json2 = fxp + cat15flat_fname+'.json'
dfc2 = pd.read_excel(fcat_xls2, sheet_name= 'A1')
dfcj2 = dfc2.to_json(fcat_json2, orient='records')

mapp_cat15f = {
    "properties": {
        "INDEX":        {"type": "keyword"  },
        "NAME":         {"type": "text"     },
        "C1LEVEL":      {"type": "integer"  },
        "C1SLUG":       {"type": "text"     },
        "C1IDEX":       {"type": "keyword"  },
        "C1IDEXPARENT": {"type": "keyword"  },
        "C1SORT":       {"type": "integer"  },
        "C1CATNAME":    {"type": "keyword"  },
        
        "C1LEVEL2":      {"type": "integer"  },
        "C2SLUG":       {"type": "text"     },
        "C2IDEX":       {"type": "keyword"  },
        "C2IDEXPARENT": {"type": "keyword"  },
        "C2SORT":       {"type": "integer"  },
        "C2CATNAME":    {"type": "keyword"  },
        
        "C1LEVEL3":      {"type": "integer"  },
        "C3SLUG":       {"type": "text"     },
        "C3IDEX":       {"type": "keyword"  },
        "C3IDEXPARENT": {"type": "keyword"  },
        "C3SORT":       {"type": "integer"  },
        "C3CATNAME":    {"type": "keyword"  },
        
        "C1LEVEL4":      {"type": "integer"  },
        "C4SLUG":       {"type": "text"     },
        "C4IDEX":       {"type": "keyword"  },
        "C4IDEXPARENT": {"type": "keyword"  },
        "C4SORT":       {"type": "integer"  },
        "C4CATNAME":    {"type": "keyword"  },
        
        "C1LEVEL5":      {"type": "integer"  },
        "C5SLUG":       {"type": "text"     },
        "C5IDEX":       {"type": "keyword"  },
        "C5IDEXPARENT": {"type": "keyword"  },
        "C5SORT":       {"type": "integer"  },
        "C5CATNAME":    {"type": "keyword"  }
        
     }
}

es.indices.delete(index='cf5')


es.indices.create(index='cf5',
    body={
      'settings': sett_mp1,
      'mappings': mapp_cat15f,

      }
    )

#cat15flat_fname = "Prod_cat_15vs_flat.xlsx"
#fcat_xls2 = fxp + cat15flat_fname
#fcat_json2 = fxp + cat15flat_fname+'.json'
#dfc2 = pd.read_excel(fcat_xls2, sheet_name= 'A1')
#dfcj2 = dfc2.to_json(fcat_json2, orient='records')

def load_cat_json2():
    with open(fcat_json2,'r') as fc:
        dfcj_data = json.load(fc) 
        for doc in dfcj_data:
            yield {
                "_id": doc['INDEX'],
                "_source": doc
                }

helpers.bulk(es, load_cat_json2(), index='cf5')
 

cat15flat_fname = "Index_cat_name.xlsx"
fcat_xls2 = fxp + cat15flat_fname
fcat_json2 = fxp + cat15flat_fname+'.json'
dfc2 = pd.read_excel(fcat_xls2, sheet_name= 'A1')
dfcj2 = dfc2.to_json(fcat_json2, orient='records')







#indeksowanie rekord po rekordzie
fc = open(fcat_json,'r')
dfcj_data = json.load(fc) 
for doc in dfcj_data:
    out = es.index( index = 'c5', id=doc['CAT0'], body = doc )
    print(doc['CAT0'], out)



sett_mp1["analysis"]["filter"]["mp_stop"]['stopwords'].append('moc')

#-------------------------------
#Wczytywanie poprzez request
headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
f = open(fxjson,'r')
json_data = json.load(f)
r = requests.post('localhost:9200/p3/_doc/_bulk', 
                  data=json_data, headers=headers, timeout=60) 
print(r)


#analiza tokenizacji

def S2(elem):  return elem[1]
#-sprawdzenie indeksowania w pętli
p = df['IDX'][0:500].to_list()
result = es.mtermvectors(index="p5", 
                         body=dict(ids=p, 
                                   parameters=dict(term_statistics=True,  
                                                   fields=["NAME"])
                                 )
                       )


f1 = open(fxp+'index_tokeny_p5.csv', 'w', encoding='UTF-8')
f1.write(fxp)
f1.write('\n'+str(datetime.now()))
f1.write('\n'+'IDX | NAME | Tokens')

for doc in result['docs']:
    pp = doc['_id']
    p1=df[df['IDX'] == pp]['NAME'].unique()[0]
    s2 = []
    s3 = []
    for x in doc['term_vectors']['NAME']['terms'].keys():
        s2.append((x,doc['term_vectors']['NAME']['terms'][x]['tokens'][0]['position'])) 
 
    s2.sort(key=S2) 
    sstr = ''
    for x in s2: 
        s3.append(x[0])
        sstr += x[0]+' '
    print(sstr)
    f1.write('\n'+pp+' | '+p1+' | '+sstr)

f1.close()












