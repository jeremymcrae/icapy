
import sys
from typing import Dict, Iterable

import requests

from icapy.config import (BASE_URL,
                          _ICA_HEADERS,
                          )
from icapy.projects import get_project_id

def get_analyses(status: str=None, max_jobs: int=5000) -> Iterable[Dict]:
    ''' find all analyses, possibly filtered by job state
    '''
    project_id = get_project_id()
    ext = f'api/projects/{project_id}/analysis:search'
    pagesize = 1000
    params = {'pageOffset': 0, 'pageSize': pagesize}
    r = requests.post(BASE_URL + ext, headers=_ICA_HEADERS, params=params)
    r.raise_for_status()
    res = r.json()
    
    # merge some statuses, since they represent stages of the same state
    states = {
        'aborted': ['aborted', 'aborting'],
        'running': ['in_progress',
                    'initializing',
                    'preparing_inputs',
                    'queued'],
        'failed': ['failed'],
        'requested': ['requested'],
        'succeeded': ['succeeded'],
    }
    
    if status is not None:
        assert status in states
    
    i = 0
    for item in res['items']:
        i += 1
        if status is not None and item['status'].lower() not in states[status]:
            continue
        yield item
        if i > max_jobs:
            break
    
    while len(res['items']) >= pagesize and i < max_jobs:
        params['pageOffset'] += pagesize
        r = requests.post(BASE_URL + ext, headers=_ICA_HEADERS, params=params)
        r.raise_for_status()
        res = r.json()
        for item in res['items']:
            i += 1
            if status is not None and item['status'].lower() not in states[status]:
                continue
            yield item

def get_analysis(analysis_id: str) -> Dict:
    ''' get details for a single analysis job
    '''
    header = {k:v for k, v in _ICA_HEADERS.items()}
    header['accept'] = 'application/vnd.illumina.v4+json'
    project_id = get_project_id()
    ext = f'api/projects/{project_id}/analyses/{analysis_id}'
    r = requests.get(BASE_URL + ext, headers=header)
    r.raise_for_status()
    return r.json()

def find_jobs(args):
    ''' command to print job info to stdout (possibly for a single status)
    '''
    statuses = ['aborted', 'running', 'failed', 'requested', 'succeeded', None]
    if args.status not in statuses:
        sys.stderr.write(f'status must be one of: {statuses}\n')
        sys.exit(1)

    if args.id is None:
        jobs = get_analyses(args.status, args.max_jobs)
    else:
        jobs = [get_analysis(args.id)]
    
    for job in jobs:
        if args.tag is not None:
            tags = set(args.tag)
            # look for any match between the supplied tags and the job tags
            if not any(bool(set(v) & tags) for v in job['tags'].values()):
                continue
            
        # jobname, job_id, time_submitted, status
        line = [job["pipeline"]["code"], 
                job["userReference"],
                job["id"],
                job['timeCreated'],
                job["status"].lower(),
                ]
        sys.stdout.write('\t'.join(line) + '\n')
