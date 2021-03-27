import requests
import os

def get_JH_uri(path, fname, branch='master'):
    website = 'https://raw.githubusercontent.com'
    repo_owner = 'CSSEGISandData'
    repo_name = 'COVID-19'
    path = f'{branch}/{path}'
    
    uri = f"{website}/{repo_owner}/{repo_name}/{path}/{fname}"
    
    return uri

def save_request_to_disk(r, path, filename):
    with open(os.path.join(path, 
                           filename), 'w') as f:
        f.write(r.text)
    
def get_daily_data(destination_folder):
    path = 'csse_covid_19_data/csse_covid_19_time_series'
    filename = 'time_series_covid19_confirmed_global.csv'
    
    daily_updates_uri = get_JH_uri(path=path,
                                   fname=filename,
                                   branch='master')
    print(f"Fetching data from {daily_updates_uri}")
    r = requests.get(daily_updates_uri)
    save_request_to_disk(r, destination_folder, filename)
    
def get_death_data(destination_folder):
    path = 'csse_covid_19_data/csse_covid_19_time_series'
    filename = 'time_series_covid19_deaths_global.csv'
    
    daily_updates_uri = get_JH_uri(path=path,
                                   fname=filename,
                                   branch='master')
    print(f"Fetching data from {daily_updates_uri}")
    r = requests.get(daily_updates_uri)
    save_request_to_disk(r, destination_folder, filename)

def get_recovery_data(destination_folder):
    path = 'csse_covid_19_data/csse_covid_19_time_series'
    filename = 'time_series_covid19_recovered_global.csv'
    
    daily_updates_uri = get_JH_uri(path=path,
                                   fname=filename,
                                   branch='master')
    print(f"Fetching data from {daily_updates_uri}")
    r = requests.get(daily_updates_uri)
    save_request_to_disk(r, destination_folder, filename)

def get_demographics_data(destination_folder):
    path = 'csse_covid_19_data'
    filename = 'UID_ISO_FIPS_LookUp_Table.csv'
    
    demographics_uri = get_JH_uri(path=path,
                                   fname=filename,
                                   branch='master')
    print(f"Fetching data from {demographics_uri}")
    r = requests.get(demographics_uri)
    save_request_to_disk(r, destination_folder, filename)

def get_JH_github_data(destination_folder='./data', 
                       get_daily=True, 
                       get_demographics=True):
    if not os.path.exists(destination_folder):
        print("Data destination directory not found, creating the folder")
        os.makedirs(destination_folder)

    if get_daily:
        get_daily_data(destination_folder)
        get_death_data(destination_folder)
        get_recovery_data(destination_folder)
    if get_demographics:
        get_demographics_data(destination_folder)
    