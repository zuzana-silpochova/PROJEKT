# ######### 0. import knihoven, které budeme potřebovat
from __future__ import division
import pandas as pd
import numpy as np


# ######### 1. načtení dat ZMĚNIT PŘED KAŽDÝM SPUŠTĚNÍM NÁZEV VSTUPNÍHO SOUBORU
items = pd.read_csv('ticket_items_201905.csv',sep=";", encoding='utf-8')


# # ########  2. vybrání pouze řádků s hodnotou "evaluated"  z původní tabulky, tj. řádky s hodnotou 'canceled' budou vyhozeny
df = items.loc[items ['evaluated']==True]

# #########  3. vyhození sloupce 'evaluated' protože nyní už máme pouze řádky s touto hodnotou a pak sloupce 'special_value' a 'winning' které nepotřebujeme
evaluated_only = df.drop(['special_value', 'winning','evaluated'], 1)

# #########  4. změna datových typů, abychom zmenšili velikost finálního souboru
for column in evaluated_only.columns:
    if evaluated_only['id_sport'].dtype == 'int64':
        evaluated_only['id_sport'] = evaluated_only['id_sport'].astype(np.int8)
    if evaluated_only['id_region'].dtype == 'int64':
        evaluated_only['id_region'] = evaluated_only['id_region'].astype(np.int8)
    if evaluated_only['id_league'].dtype == 'int64':
        evaluated_only['id_league'] = evaluated_only['id_league'].astype(np.int8)
    if evaluated_only['id_match'].dtype == 'int64':
        evaluated_only['id_match'] = evaluated_only['id_match'].astype(np.int32)
    if evaluated_only['id_market'].dtype == 'int64':
        evaluated_only['id_market'] = evaluated_only['id_market'].astype(np.int8)
    if evaluated_only['id_client'].dtype == 'int64':
        evaluated_only['id_client'] = evaluated_only['id_client'].astype(np.int32)

# ######## 5. zápis do mezisouboru (prostě nevim jak jinak to vyřešit :D )
evaluated_only.to_csv('clean.csv', index= False) 

# ######## 6. rozdělení čistého souboru na dva

numrows = 35000000 #počet řádků tak aby výsledný soubor měl méně než 5 GB
count = 0 #na hlídání počtu kusů
chunkrows = 100000 #čte 100k řádků najednou
casti = pd.read_csv('clean.csv', iterator=True, chunksize=chunkrows) 
for chunk in casti: #pro každých 100k řádků
    if count <= numrows/chunkrows: #dokud nedosáhne stanoveného počtu řádků 
        outname = "TI_201905_C1.csv" # !!!ZMĚNIT PŘED KAŽDÝM SPUŠTĚNÍM NÁZEV VÝSTUPNÍHO SOUBORU
    else:
        outname = "TI_201905_C2.csv" # !!!ZMĚNIT PŘED KAŽDÝM SPUŠTĚNÍM NÁZEV VÝSTUPNÍHO SOUBORU
    
    chunk.to_csv(outname, mode='a', header=['TICKET_ID', 'ID_SPORT', 'ID_REGION', 'ID_LEAGUE', 'ID_MATCH', 'ID_MARKET', 'ODDS_VALUE', 'ID_CLIENT'], index=False)
    count+=1
