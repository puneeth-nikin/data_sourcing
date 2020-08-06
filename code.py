import pandas as pd
import requests
url = 'https://www.quantconnect.com/processDataDownload/22024/f06d6441f9c3785dd441b3db7b9972825f099fbcb61b7c5ba369a5fd5f09547b/20200322'
r = requests.get(url, stream= True)
print(r.text)

def download_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)
download_url(url,'test/test')

import zipfile
with zipfile.ZipFile('test/test', 'r') as zip_ref:
    zip_ref.extractall('test')