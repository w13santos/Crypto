# -*- coding: utf-8 -*-
"""
Created on Thu Jun 13 11:36:15 2019 
@author: washington zanoni
@email:xdata.science@gmail.com
fonte:   https://blinktrade.com/docs/?shell#symbols
Exemplo: https://bitcambio_api.blinktrade.com/api/v1/BRL/ticker?crypto_currency=BTC
Necessário criar diretório C:/blockchain/cot_bitcambio
Código busca cotção do BTC/BRL a cada 20s estruturando num csv
"""

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

def job_get_cot_bitcambio():
            url = 'https://bitcambio_api.blinktrade.com/api/v1/BRL/ticker?crypto_currency=BTC'
            arq_json = requests.get(url).json()   
            data = [arq_json]
            df = pd.DataFrame.from_dict(json_normalize(data), orient='columns')
            df['time_stamp'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df.to_csv('C:/blockchain/cot_bitcambio/base_cot_bitcambio.csv', mode='a', sep=';', index = False, header=False)
            print(df)
            
job_get_cot_bitcambio()


# In[1]:    
print('Inicio captura cotação Bitcambio...')
schedule.every(0.2).minutes.do(job_get_cot_bitcambio)
while True:
        try:            
            schedule.run_pending()
            time.sleep(1)
        except:
            pass
        




