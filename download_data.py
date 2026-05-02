import os
import urllib.request

DATA_DIR = 'data'
os.makedirs(DATA_DIR, exist_ok=True)

DATASETS = {
    'crime.csv': 'https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD',
    'police_stations.csv': 'https://data.cityofchicago.org/api/views/z8bn-74gv/rows.csv?accessType=DOWNLOAD',
    'arrests.csv': 'https://data.cityofchicago.org/api/views/dpt3-jri9/rows.csv?accessType=DOWNLOAD',
    'violence.csv': 'https://data.cityofchicago.org/api/views/gumc-mgzr/rows.csv?accessType=DOWNLOAD',
    'sex_offenders.csv': 'https://data.cityofchicago.org/api/views/vc9r-bqvy/rows.csv?accessType=DOWNLOAD'
}

def download_file(filename, url):
    path = os.path.join(DATA_DIR, filename)
    print(f'Downloading {filename}...')
    try:
        # Check size first
        req = urllib.request.Request(url, method='HEAD')
        with urllib.request.urlopen(req) as resp:
            size = resp.headers.get('Content-Length')
            if size:
                print(f'Size of {filename}: {int(size) / (1024*1024):.2f} MB')
            else:
                print(f'Size of {filename}: Unknown (chunked)')
        
        # Now download
        urllib.request.urlretrieve(url, path)
        print(f'Finished downloading {filename}')
    except Exception as e:
        print(f'Failed to download {filename}: {e}')

if __name__ == '__main__':
    for name, url in DATASETS.items():
        download_file(name, url)
