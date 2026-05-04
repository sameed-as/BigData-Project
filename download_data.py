import os
import subprocess

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
    print(f'\n========================================')
    print(f'Downloading {filename}...')
    print(f'========================================')
    
    # Use curl with a progress bar (-#), follow redirects (-L), and save to file (-o)
    try:
        subprocess.run(['curl', '-#', '-L', '-o', path, url], check=True)
        print(f'Successfully downloaded {filename}\n')
    except subprocess.CalledProcessError as e:
        print(f'Failed to download {filename}: {e}\n')

if __name__ == '__main__':
    for name, url in DATASETS.items():
        download_file(name, url)
