import pandas as pd

import zipfile
import requests
import time
import os
url = 'https://www.quantconnect.com/processDataDownload/22024/f06d6441f9c3785dd441b3db7b9972825f099fbcb61b7c5ba369a5fd5f09547b/20200322'
r = requests.get(url, stream= True)


def download_url(url, save_path,zip_dest, chunk_size=128):
    url=url['Links']
    r = requests.get(url, stream=True)
    with open(save_path+'/zip', 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

    with zipfile.ZipFile(save_path+'/zip', 'r') as zip_ref:
         zip_ref.extractall(zip_dest)

def combine_organise(zip_path):
    for file in os.listdir(zip_path):
        if file.endswith(".csv"):
            print(os.path.join(zip_path, file))



def source(symbol,freq):
    source_path='symbol_url/symbols/'+symbol+'/'+freq+'/'+symbol+'.csv'
    dest_path = 'symbol_url/symbols/' + symbol + '/' + freq + '/' + 'data'
    zip_path='unziped_files/symbols/' + symbol + '/' + freq + '/' + 'data'
    source_links=pd.read_csv(source_path)
    for index,row in source_links.iterrows():
        try:
            download_url(row,dest_path,zip_path)
        except:
            time.sleep(5)
            continue
        # source_links.apply(lambda x:download_url(x,dest_path),axis=1)



source('AUDCHF','minute')