import pandas as pd

import zipfile
import requests
import time
import os
import shutil


def download_url(url, symbol, chunk_size=128):
    url=url['Links']
    save_path = 'symbol_url/' + symbol
    zip_dest = 'unzipped_files/' + symbol + '/minute'
    r = requests.get(url, stream=True)
    with open(save_path+'/zip', 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

    with zipfile.ZipFile(save_path+'/zip', 'r') as zip_ref:
         zip_ref.extractall(zip_dest)

def source_daily(symbol):
    save_path='symbol_url/' + symbol + '/' + symbol.lower() + '.zip'
    zip_path='unzipped_files/'+symbol+'/daily'
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
        data['Date']=data['Date'].dt.strftime('%Y/%m/%d')
        data = data[['Date', 'Open', 'High', 'Low', 'Close']]
        data.to_csv('symbols/'+symbol+'/'+symbol+'_daily.csv',index=False)

def combine_organise(symbol):
    final_data=pd.DataFrame(columns=['Date','sec','Open','High','Low','Close','Volume'])
    zip_path = 'unzipped_files/' + symbol + '/minute'
    for file in os.listdir(zip_path):

        try:
            if file.endswith(".csv"):
                print(file)
                path=os.path.join(zip_path, file)
                date=file[:8]
                data = pd.read_csv(path,header=None)

                data = data[range(0,6,1)]
                data.columns=['sec','Open','High','Low','Close','Volume']

                data['Date']=date
                data['Volume']=4
                if data.shape[0]>2:
                    final_data=final_data.append(data)

        except:
            continue

    final_data['Date']=pd.to_datetime(final_data['Date'], format='%Y%m%d')+pd.to_timedelta(final_data['sec']/1000,unit='s')
    final_data = final_data.sort_values(by='Date')
    final_data['Time']=final_data['Date'].dt.strftime('%H:%M')
    final_data['Date'] = final_data['Date'].dt.strftime('%Y.%m.%d')
    final_data=final_data[['Date','Time','Open','High','Low','Close','Volume']]
    final_data.to_csv('symbols/'+symbol+'/'+symbol+'_minute.csv',index=False,header=False)
    print(final_data)





def source(symbol):

    dirName='symbols/'+symbol
    if not os.path.exists(dirName):
        os.makedirs(dirName)
        print("Directory ", dirName, " Created ")
    else:
        print("Directory ", dirName, " already exists")

    dirName='unzipped_files/'+symbol+'/daily'
    if not os.path.exists(dirName):
        os.makedirs(dirName)
        print("Directory ", dirName, " Created ")
    else:
        print("Directory ", dirName, " already exists")

    dirName='unzipped_files/'+symbol+'/minute'
    if not os.path.exists(dirName):
        os.makedirs(dirName)
        print("Directory ", dirName, " Created ")
    else:
        print("Directory ", dirName, " already exists")

    dirName = 'zipfiles'
    if not os.path.exists(dirName):
        os.makedirs(dirName)
        print("Directory ", dirName, " Created ")
    else:
        print("Directory ", dirName, " already exists")

    source_path='symbol_url/'+symbol+'/'+symbol+'_minute.csv'
    print(source_path)
    source_links=pd.read_csv(source_path)


    for index,row in source_links.iterrows():
        try:
            download_url(row,symbol)
        except:
            time.sleep(5)
            continue
    combine_organise(symbol)
    source_daily(symbol)
    shutil.make_archive('zipfiles/' + symbol, 'zip', root_dir='symbols/'+symbol+'/')




# symbol= "AUDCHF"
# source(symbol)
# combine_organise(symbol)

#source_daily(symbol)


