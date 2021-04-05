#!/usr/bin/env python3<
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 29 11:46:50 2019
API request to www.openliga.de requesting current bundesliga scores
Write parsed data into mySQL Database
"""
import requests
import pandas as pd
from sqlalchemy import create_engine

# Construct URLs for each Bundesliga matchday API Call
spieltag_urls = []
spieltag = 1

while spieltag <= 34:
    spieltag_urls.append('http://www.openligadb.de/api/getmatchdata/bl1/2019/'+str(spieltag))
    spieltag = spieltag + 1

# Call API and parse data
saison = []
for i in spieltag_urls:  
    # Call API
    r = requests.get(url=i) 
    data = (r.json())
    
    # Parse data
    j = 0
    spieltag = []
    home = []
    away = []
    time = []
    score_home = []
    score_away = []
    status = []
    
    while j <= 8:
        spieltag.append(data[j]['Group']['GroupName'])
        time.append(data[j]['MatchDateTime'])
        home.append(data[j]['Team1']['TeamName'])
        away.append(data[j]['Team2']['TeamName'])
        status.append(data[j]['MatchIsFinished'])
        if data[j]['Goals']:
            score_home.append(data[j]['Goals'][-1]['ScoreTeam1'])
            score_away.append(data[j]['Goals'][-1]['ScoreTeam2'])
        else:
            score_home.append(0)
            score_away.append(0)
        j = j+1
    
    # construct pandas dataframe
    df = pd.DataFrame()
    df['spieltag'] = spieltag
    df['home'] = home
    df['away'] = away
    df['time'] = time
    df['tor_home'] = score_home
    df['tor_away'] = score_away
    df['status'] = status
    saison.append(df)

# merge all dataframes
spielplan = pd.concat(saison, ignore_index = True)

# recode teamnames
dummy_list = ['home', 'away']
for i in dummy_list:
    spielplan.loc[spielplan[i] == 'FC Bayern', i]                 = 'FCB'
    spielplan.loc[spielplan[i] == 'Borussia Dortmund', i]         = 'BVB' 
    spielplan.loc[spielplan[i] == 'RB Leipzig', i]                = 'RBL' 
    spielplan.loc[spielplan[i] == 'Bayer Leverkusen', i]          = 'B04' 
    spielplan.loc[spielplan[i] == 'Borussia Mönchengladbach', i]  = 'BMG' 
    spielplan.loc[spielplan[i] == 'VfL Wolfsburg', i]             = 'VFL' 
    spielplan.loc[spielplan[i] == 'TSG 1899 Hoffenheim', i]       = 'TSG' 
    spielplan.loc[spielplan[i] == 'Eintracht Frankfurt' ,i]       = 'SGE' 
    spielplan.loc[spielplan[i] == 'Werder Bremen', i]             = 'SVW' 
    spielplan.loc[spielplan[i] == 'Fortuna Düsseldorf', i]        = 'F95' 
    spielplan.loc[spielplan[i] == 'Hertha BSC', i]                = 'BSC' 
    spielplan.loc[spielplan[i] == '1. FSV Mainz 05', i]           = 'FSV' 
    spielplan.loc[spielplan[i] == 'SC Freiburg', i]               = 'SCF' 
    spielplan.loc[spielplan[i] == 'FC Schalke 04', i]             = 'S04' 
    spielplan.loc[spielplan[i] == 'FC Augsburg', i]               = 'FCA' 
    spielplan.loc[spielplan[i] == '1. FC Köln', i]                = 'FCK' 
    spielplan.loc[spielplan[i] == 'SC Paderborn 07', i]           = 'SCP' 
    spielplan.loc[spielplan[i] == '1. FC Union Berlin', i]        = 'FCU' 

# recode match-status
df.loc[df['status'] == False, 'status'] = 0
df.loc[df['status'] == True, 'status'] = 1

# recode matchday date
spielplan.spieltag = spielplan.spieltag.str.extract('(\d+)')
spielplan.spieltag = pd.to_numeric(spielplan.spieltag)

# recode kickoff datetime
spielplan.time = pd.to_datetime(spielplan.time)

# write pandas dataframe into mySQL database
engine = create_engine('mysql+mysqlconnector://XXX:XXX@localhost:XXX/XXX', echo=False)
spielplan.to_sql(name='bundesliga_spielplan', con=engine, if_exists = 'replace', index=False)