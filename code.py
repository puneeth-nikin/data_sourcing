import pandas as pd

import zipfile
import requests
url = 'https://www.quantconnect.com/processDataDownload/22024/f06d6441f9c3785dd441b3db7b9972825f099fbcb61b7c5ba369a5fd5f09547b/20200322'
r = requests.get(url, stream= True)


def download_url(url, save_path, chunk_size=128):
    url=url['Links']
    r = requests.get(url, stream=True)
    try:
        with open(save_path+'/zip', 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)

        with zipfile.ZipFile(save_path+'/zip', 'r') as zip_ref:
             zip_ref.extractall(save_path)
    except:
        pass


def source(symbol,freq):
    source_path='symbol_url/symbols/'+symbol+'/'+freq+'/'+symbol+'.csv'
    dest_path = 'symbol_url/symbols/' + symbol + '/' + freq + '/' + 'data'
    source_links=pd.read_csv(source_path)
    source_links.apply(lambda x:download_url(x,dest_path),axis=1)


source('AUDCHF','minute')