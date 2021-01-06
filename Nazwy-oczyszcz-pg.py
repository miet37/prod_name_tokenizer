# -*- coding: utf-8 -*-
"""
Created on Mon Jan 13 19:51:31 2020

@author: user
"""

#import re
#import collections, pprint
import pandas as pd



letters = 'aąbcćdeęfghijklłmnńoópqrsśtuvwxyzżź'

f_in1 = 'C:\\MP\\aa\\Nazwy\\Nazwa i prod grupujący_pp'

df = pd.read_excel(f_in1+'.xlsx', sheet_name='Arkusz1').astype(str)
print('Wczytano', df.shape)

df['Nazwa'] = df['Nazwa'].str.lower()  
df['Pg'] = ''
df['R'] = ''

i=0
for r in df['Nazwa']:
    name_as_words = r.split()
    pr_name = ''
    pr_r = ''
    for x in name_as_words:
        if len(x)>2 and all(c in letters for c in x):      
            pr_name += x + " "
        else:
            pr_r += x + " "
    
    df['Pg'][i] = pr_name  
    df['R'][i] = pr_r
    i += 1
    if i%1000 == 0:
        print(i, pr_name)

print(df.shape)

df = df.drop_duplicates()

df.to_excel(f_in1+'_pg.xlsx')