

import sys
from typing import Dict, Iterable

import requests

from icapy.config import (BASE_URL,
                          load_config,
                          write_config,
                          get_ica_key,
                          get_tenant,
                          _ICA_HEADERS,
                          )

def get_projects() -> Iterable[Dict]:
    ''' find all projects
    '''
    ext = 'api/projects'
    pagesize = 10
    params = {'pageOffset': 0, 'pageSize': pagesize}
    r = requests.get(BASE_URL + ext, headers=_ICA_HEADERS, params=params)
    r.raise_for_status()
    res = r.json()
    
    for item in res['items']:
        yield item

    while len(res['items']) >= pagesize:
        params['pageOffset'] += pagesize
        r = requests.get(BASE_URL + ext, headers=_ICA_HEADERS, params=params)
        r.raise_for_status()
        res = r.json()
        for item in res['items']:
            yield item

def list_projects(*args):
    ''' command to list projects to stdout
    '''
    for project in get_projects():
        # project name and ID
        line = [project["name"],
                project["id"],
                ]
        sys.stdout.write('\t'.join(line) + '\n')

def set_default_project(*args):
    ''' set the default project
    '''
    projects = [(x['name'], x['id']) for x in get_projects()]
    names, ids = zip(*projects)
    if len(names) == 0:
        sys.stderr.write('no projects available to select from\n')
        sys.exit(1)
        
    if len(names) == 1:
        sys.stderr.write(f'one project available, setting as default ({names[0]})')
        selection = 0
    else:
        sys.stderr.write("set default project:\n")
        for i, name in enumerate(names):
            sys.stderr.write(f' {i + 1}: {name}\n')
        while True:
            selection = input("provide project number to set as default: ")
            if not str.isnumeric(selection):
                sys.stderr.write('must provide integer\n')
                continue
            selection = int(selection) - 1
            if selection < 0 or selection > len(ids) - 1:
                sys.stderr.write('choice must be one of the displayed options\n')
                for i, name in enumerate(names):
                    sys.stderr.write(f' {i + 1}: {name}\n')
                continue
            
            break
    
    config = load_config()
    if 'tenant' not in config:
        config['tenant'] = get_tenant()
    
    if 'ica_api_key' not in config:
        config['ica_api_key'] = get_ica_key()
    
    config['ica_project_id'] = ids[selection]
    config['ica_project_name'] = names[selection]
    
    write_config(config)

def set_default_project(*args):
    ''' set the default project
    '''
    projects = [(x['name'], x['id']) for x in get_projects()]
    names, ids = zip(*projects)
    if len(names) == 0:
        sys.stderr.write('no projects available to select from\n')
        sys.exit(1)
        
    if len(names) == 1:
        sys.stderr.write(f'one project available, setting as default ({names[0]})')
        selection = 0
    else:
        sys.stderr.write("set default project:\n")
        for i, name in enumerate(names):
            sys.stderr.write(f' {i + 1}: {name}\n')
        while True:
            selection = input("provide project number to set as default: ")
            if not str.isnumeric(selection):
                sys.stderr.write('must provide integer\n')
                continue
            selection = int(selection) - 1
            if selection < 0 or selection > len(ids) - 1:
                sys.stderr.write('choice must be one of the displayed options\n')
                for i, name in enumerate(names):
                    sys.stderr.write(f' {i + 1}: {name}\n')
                continue
            
            break
    
    config = load_config()
    if 'tenant' not in config:
        config['tenant'] = get_tenant()
    
    if 'ica_api_key' not in config:
        config['ica_api_key'] = get_ica_key()
    
    config['ica_project_id'] = ids[selection]
    config['ica_project_name'] = names[selection]
    
    write_config(config)

def get_project_id() -> str:
    config = load_config()
    if 'ica_project_id' not in config:
        set_default_project()
        config = load_config()
    
    return config['ica_project_id']
