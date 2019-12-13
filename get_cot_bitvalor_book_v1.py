# -*- coding: utf-8 -*-
"""
Created on Mon Jul  1 11:34:51 2019
@author: washington zanoni
@email: xdata.science@gmail.com
Esse script retorna informações de estatistica do book de ofertas
fonte> https://api.bitvalor.com/v1/order_book_stats.json
"""

import requests
import pandas as pd
import datetime
import csv
import json
from pandas.io.json import json_normalize


path_csv  = 'C:/blockchain/cot_bitvalor/base_cot_bitvalor_book.csv'

def Job():
    today = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    url = 'https://api.bitvalor.com/v1/order_book_stats.json'
    arq_json = requests.get(url).json()
    
    df = pd.DataFrame(arq_json)
    df_T = df.T
    df_T['timestamp'] = today
    
    df_T.to_csv(path_csv, mode='a', sep=';', index = False, header=False) 
    
Job()