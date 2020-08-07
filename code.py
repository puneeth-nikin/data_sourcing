import pandas as pd

import zipfile
import requests
import time
import os


def download_url(url, symbol, chunk_size=128):
    url=url['Links']
    save_path = 'symbol_url/symbols/' + symbol + '/minute/' + 'data'
    zip_dest = 'unzipped_files/symbols/' + symbol + '/minute/' + 'data'
    r = requests.get(url, stream=True)
    with open(save_path+'/zip', 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

    with zipfile.ZipFile(save_path+'/zip', 'r') as zip_ref:
         zip_ref.extractall(zip_dest)

def source_daily(symbol):
    save_path='symbol_url/symbols/' + symbol + '/daily/' + symbol.lower() + '.zip'
    zip_path='unzipped_files/symbols/'+symbol+'/daily'
    with zipfile.ZipFile(save_path, 'r') as zip_ref:
         zip_ref.extractall(zip_path)
    for file in os.listdir(zip_path):
        print(file)
        path = os.path.join(zip_path, file)
        data = pd.read_csv(path, header=None)

        data = data[range(0, 6, 1)]
        data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        data['Volume'] = 4
        data['Date'] = pd.to_datetime(data['Date'], format='%Y%m%d %H:%M')
        data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        data.to_csv('symbols/'+symbol+'/daily/'+symbol+'_daily.csv',index=False)

def combine_organise(symbol):
    final_data=pd.DataFrame(columns=['Date','Open','High','Low','Close','Volume'])
    zip_path = 'unzipped_files/symbols/' + symbol + '/minute/' + 'data'
    df=pd.DataFrame(columns=['path','file'])
    for file in os.listdir(zip_path):
        print(file)
        try:
            if file.endswith(".csv"):
                path=os.path.join(zip_path, file)
                date=file[:8]
                df=df.append({'path':path,'file':file},ignore_index=True)
                data = pd.read_csv(path,header=None)

                data = data[range(0,6,1)]
                data.columns=['sec','Open','High','Low','Close','Volume']
                data['Date']=date
                data['Volume']=4
                data['Date']=pd.to_datetime(data['date'], format='%Y%m%d')+pd.to_timedelta(data['sec']/1000,unit='s')
                data=data[['Date','Open','High','Low','Close','Volume']]
                if data.shape[0]>2:
                    final_data=final_data.append(data)

        except:
            continue
    final_data = final_data.sort_values(by='Date')
    final_data.to_csv('symbols/'+symbol+'/minute/'+symbol+'_minute.csv',index=False)
    print(final_data)






def source(symbol):
    source_path='symbol_url/symbols/'+symbol+'/minute/'+symbol+'.csv'
    source_daily(symbol)

    source_links=pd.read_csv(source_path)
    for index,row in source_links.iterrows():
        try:
            download_url(row,symbol)
        except:
            time.sleep(5)
            continue
    combine_organise(symbol)

symbol= "AUDCHF"
source(symbol)