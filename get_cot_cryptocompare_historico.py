# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 10:55:15 2019
@author: washington zanoni
@email: xdata.science@gmail.com
API fonte: https://min-api.cryptocompare.com/documentation?key=Historical&cat=dataHistohour
API exemplo: https://min-api.cryptocompare.com/data/histohour?fsym=BTC&tsym=USD&limit=10
Descrição: Retorno de cotação moeda digitai para fiat de hora em hora no periodo histórico desde determinado
Moedas digitais: ['BTC','ETH','XRP','LTC','BCH','NEO','ADA','BNB','XLM','EOS','XMR','DASH','TRX','MTL','ALGO','ETC','ZEC','BTG','NANO','WAVES','THETA','TUSD']
Moedas fiat: ['USD','EUR','BRL','MEX','AUD','ARG','GBP','CNY','CAN','SEK','CHF','KRW']
"""

import time
import requests
import pandas as pd
import datetime
import json
from pandas.io.json import json_normalize


# set de variáveis ---------------------------------------------------------------------------------------------------#
timestamp = datetime.datetime.now()
path_csv  = 'C:/blockchain/cot_cryptocompare/'+timestamp.strftime("%Y%m%d%H%M%S")+'.csv'
coin_list = ['BTC','ETH'] #,'XRP','LTC','BCH','NEO','ADA','BNB','XLM','EOS','XMR','DASH','TRX','MTL','ALGO','ETC','ZEC','BTG','NANO','WAVES','THETA','TUSD']
coin_list2 = ['USD','EUR'] #,'BRL','MEX','AUD','ARG','GBP','CNY','CAN','SEK','CHF','KRW']
#---------------------------------------------------------------------------------------------------------------------#

# ''' Busca historico '''

# Passa url da API que retorna um Json. 
# O método retona um DataFrame   
def JsonToDF():
    df = pd.DataFrame(columns=['close','high','low','open','time','volumefrom','volumeto'])
    v_timestamp_now    = int(time.time())
    v_dt_cont =     (datetime.datetime.utcfromtimestamp(v_timestamp_now).strftime('%Y-%m-%d %H:%M:%S'))
    try:        
            while v_dt_cont > '2019-12-12 00:00:00':     # aqui determina a data início
                print('while')
                for index, item in enumerate(coin_list):
                    print('for 1')
                    for index2, item2 in enumerate(coin_list2):
                        print('for 2')
                        try:
                            url = 'https://min-api.cryptocompare.com/data/histohour?fsym='+item+'&tsym='+item2+'&limit=2000&toTs='+str(v_timestamp_now)
                            print(url)
                            arq_json = requests.get(url).json()
                            data = arq_json.get("Data")
                            dff = pd.DataFrame.from_dict(json_normalize(data), orient='columns')                          
                            dff['coin']=item
                            dff['coin_fiat']=item2                        
                            dff['date'] = pd.to_datetime(dff['time'],unit='s')    
                            v_timestamp_now = dff['time'].iloc[1]
                            v_dt_cont = (datetime.datetime.utcfromtimestamp(v_timestamp_now).strftime('%Y-%m-%d %H:%M:%S'))
                            time.sleep(1)        
                            df = pd.concat([df,dff])  
                            print(item+'/'+item2)    
                        except:
                                pass
    except:
            pass
    df = df.sort_values(by=['time'],ascending=True)       
    df.to_csv(path_csv , mode='a', sep=';', index = False, header=False) 
    
JsonToDF()   


#---------------------------------------------------------------------------------------------------------------------#


    