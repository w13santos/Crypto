# -*- coding: utf-8 -*-
'''

@author: washington.zanoni
@email: xdata.science@gmail.com

Descrição:
   Código pega dados de cotação de criptoativos via da API Coindesck
   Nesse código exemplo foi criado um agendamento para rodar a cada 20s
	É necessario criar pastas em seu computador
    No final da execução é criado um arquivo csv com linhas e colunas
    Gera csv incremental com as cotaçóes
Fonte:
	https://www.coindesk.com/
End-point:
    https://api.coindesk.com/v1/bpi/currentprice/{sigla+cryptoativo}.json

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



# In[2]:
def job():
    df = pd.DataFrame(columns=['coin', 'dt', 'src', 'price'])
    coin_list = ['USD','BRL','ARS','SGD','AUD','GBP','CLP','MXN','EUR','JPY','CNY','CAD','SEK','CHF']
    for index, item in enumerate(coin_list):
        url = 'https://api.coindesk.com/v1/bpi/currentprice/'+item+'.json'
        arq_json    = requests.get(url).json()
        coin_origem = '1'
        coin_cod    = item
        coin_time   = str(datetime.datetime.strptime(((arq_json['time']['updatedISO']).replace("T"," ").replace("+00:00","")),'%Y-%m-%d %H:%M:%S'))
        coin_value  = float((arq_json['bpi'][item]['rate']).replace(",",""))

        dict1 = {'coin':[item],'dt':[coin_time],'src':[coin_origem],'price':[coin_value]}
        df_dict = pd.DataFrame(dict1)

        df = pd.concat([df,df_dict])
        print('df = pd.concat([df,df_dict])')
    try:
        df.to_csv('c:/blockchain/cot_coindesk/base_cot_coindesk.csv', mode='a', sep=';', index = False, header=False)
    except:
        print('except')
        pass

# In[ ]:
print('Inicio... Captura dados API Coindesck...')       
schedule.every(0.2).minutes.do(job)
while True:
        schedule.run_pending()
        time.sleep(1)
        print('.')

