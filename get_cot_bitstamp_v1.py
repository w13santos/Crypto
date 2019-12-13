# -*- coding: utf-8 -*-
'''

@author: washington.zanoni
@email: xdata.science@gmail.com

Descrição:
   Código pega dados de cotação de criptoativos via da API Bitstamp
   Nesse código exemplo foi criado um agendamento para rodar a cada 20s
	É necessario criar pastas em seu computador
    No final da execução é criado um arquivo csv com linhas e colunas
    Gera csv incremental com as cotaçóes
Fonte:
    https://www.bitstamp.net/api
End-point:
    https://www.bitstamp.net/api/v2/transactions/btcusd/APIv2

'''

# In[1]:

import schedule
import time
import requests
import pandas as pd
import os
import datetime
import decimal
import csv
import json
from pandas.io.json import json_normalize

# In[1]:
coin_list = ['btcusd', 'btceur', 'eurusd', 
             'xrpusd', 'xrpeur', 'xrpbtc', 
             'ltcusd', 'ltceur', 'ltcbtc', 
             'ethusd', 'etheur', 'ethbtc', 
             'bchusd', 'bcheur', 'bchbtc']

# In[1]:

def job_get_cot_bitstamp():
        for index, item in enumerate(coin_list):
            url = 'https://www.bitstamp.net/api/v2/ticker/'+item
            arq_json    = requests.get(url).json()   
            data = [arq_json]
            df = pd.DataFrame.from_dict(json_normalize(data), orient='columns')
            df['coin'] = item
            df.to_csv('/root/ingestion/base/base_cot_bitstamp.csv', mode='a', sep=';', index = False, header=False)
            #print(df)

job_get_cot_bitstamp()

'''            
# In[1]:    
print('Inicio captura cotação Bitstamp...')
schedule.every(0.2).minutes.do(job_get_cot_bitstamp)
while True:
        try:            
            schedule.run_pending()
            time.sleep(1)
        except:
            pass
'''            
        

    










