from bs4 import BeautifulSoup
import os
import re
import requests
import sys
import wget

def download_folder(folder_num):
    url = 'https://portal.nersc.gov/project/cosmo/data/CatWISE/2020/'
    
    if 0 <= folder_num < 10:
        folder_url = url + '00' + str(folder_num)
    elif 10 <= folder_num < 100:
        folder_url = url + '0' + str(folder_num)
    else:
        folder_url = url + str(folder_num)

    folder_url += '/'
    print('Downloading from ' + folder_url)
    response = requests.get(folder_url)

    soup = BeautifulSoup(response.text, 'html.parser')
    links = [a.get('href') for a in soup.find_all('a', href=True)]

    for el in links[5::]:
        if os.path.exists('/data/raw2/' + el):
            continue
        else:
            wget.download(folder_url + el, '/data/raw2/' + el)

if __name__ == '__main__':
    start, end = int(sys.argv[1]), int(sys.argv[2])
    for i in range(start, end):
        download_folder(i)

