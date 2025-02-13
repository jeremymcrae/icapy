

from getpass import getpass
import json
import os
from pathlib import Path
import stat
from typing import Dict

import requests

BASE_URL = 'https://ica.illumina.com/ica/rest/'

def get_config_path() -> Path:
    config_folder = Path.home() / ".config" / "ica"
    config_folder.mkdir(parents=True, exist_ok=True)
    return config_folder / "config.json"

def load_config() -> Dict[str, str]:
    ''' load configuration data
    '''
    config_path = get_config_path()
    if config_path.exists():
        return json.load(open(config_path))
    else:
        return {}

def write_config(config: Dict[str, str]):
    ''' write configuration data to disk
    '''
    config_path = get_config_path()
    with open(config_path, 'wt') as handle:
        json.dump(config, handle, indent=True)
    
    # ensure user only read/write
    os.chmod(config_path, stat.S_IRUSR | stat.S_IWUSR)

def get_ica_key() -> str:
    config = load_config()
    if 'ica_api_key' not in config:
        key = getpass('provide ICA API key: ')
        # TODO: check the API key is valid before storing it
        config['ica_api_key'] = key
        write_config(config)
    
    return config['ica_api_key']

def get_tenant() -> str:
    config = load_config()
    if 'tenant' not in config:
        config['tenant'] = input('provide your ICA tenant ID: ')
        # TODO: check the tenant ID is valid
        write_config(config)
    
    return config['tenant']

def get_token() -> str:
    ''' get JWT token for authentication
    '''
    tenant = get_tenant()
    api_key = get_ica_key()
    
    ext = f'api/tokens'
    params = {'tenant': tenant}
    headers = {
        'X-API-Key': api_key,
        'accept': 'application/vnd.illumina.v3+json'
    }
    r = requests.post(BASE_URL + ext, headers=headers, params=params, stream=True)
    r.raise_for_status()
    return r.json()['token']

if "_ICA_HEADERS" not in globals():
    _ICA_HEADERS = {'x-api-key': get_ica_key(),
                'Content-Type': 'application/json'}
