

import io
import json
import os
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, Union, Tuple

import requests

from icapy.config import (BASE_URL,
                          _ICA_HEADERS,
                          )
from icapy.projects import get_project_id

def get_data(path: Path | str) -> Iterable[Union[str, None]]:
    ''' get a data ID matching a given path in a project
    '''
    project_id = get_project_id()
    ext = f'api/projects/{project_id}/data'
    
    params = {}
    path = Path(path)
    parent = path.parent
    name = path.name
    
    params['parentFolderPath'] = str(parent)
    if not params['parentFolderPath'].endswith('/'):
        params['parentFolderPath'] += '/'
    if name != '':
        params['filename'] = name
        params['filenameMatchMode'] = 'EXACT'
    
    r = requests.get(BASE_URL + ext, params=params, headers=_ICA_HEADERS)
    r.raise_for_status()
    data = r.json()['items']
    if len(data) == 0:
        raise ValueError(f'unknown path: {path}')
    
    for item in data:
        if item['data']['details']['path'].rstrip('/') == str(path).rstrip('/'):
            yield item

def delete_file(path: str, recursive=False):
    ''' delete a file object
    '''
    if path is None:
        raise ValueError('cannot delete files without supplying a path')
    elif path == '/':
        raise ValueError('cannot delete root directory')
    
    project_id = get_project_id()
    for item in get_data(path):
        data_id = item['data']['id']
        if item['data']['details']['dataType'] == 'FOLDER' and not recursive:
            sys.stderr.write(f"cannot remove '{path}': Is a directory\n")
            sys.exit(1)
        
        ext = f'api/projects/{project_id}/data/{data_id}:delete'
        # sys.stdout.write(f'will remove {path} via {ext}')
        r = requests.post(BASE_URL + ext, headers=_ICA_HEADERS)
        r.raise_for_status()

def rm_wrapper(args):
    ''' converts CLI arguments into fucntion call for deleting file/folder
    '''
    try:
        delete_file(args.PATH, args.recursive)
    except ValueError as err:
        sys.stderr.write(err.args[0] + '\n')
        sys.exit(1)

def mv(old_path: str, new_path: str):
    raise NotImplementedError

def get_object_details(item: Dict[str, Any]):
    ''' get details of an object
    '''
    return {
        'name': item['data']['details']['name'],
        'path': item['data']['details']['path'],
        'created_date': item['data']['details']['timeCreated'],
        'size': item['data']['details']['fileSizeInBytes'],
        'id': item['data']['id'],
    }

def list_files(path: str, pattern: str=None) -> Iterable[Dict[str, Any]]:
    ''' list details for file or folder contents
    '''
    
    project_id = get_project_id()
    pagesize = 1000
    header = {k: v for k, v in _ICA_HEADERS.items()}
    
    data = [{'data': {'id': None, 'details': {'dataType': None}}}]
    if str(path) != '/' and path is not None:
        data = get_data(path)
    
    for item in data:
        if item['data']['details']['dataType'] == 'FILE':
            # if the path corresponds to a single file, yield that without
            # making extra checks
            yield get_object_details(item)
            continue
        
        data_id = item['data']['id']
        params = {'pageOffset': 0, 'pageSize': pagesize}
        if data_id is None:
            params['parentFolderPath'] = '/'
        else:
            params['parentFolderId'] = data_id
        
        if pattern is not None:
            params['filename'] = pattern
            params['filenameMatchMode'] = 'FUZZY'
        
        ext = f'api/projects/{project_id}/data'
        r = requests.get(BASE_URL + ext, params=params, headers=header)
        r.raise_for_status()
        res = r.json()
        
        for item in res['items']:
            yield get_object_details(item)
        
        while len(res['items']) >= pagesize:
            params['pageOffset'] += pagesize
            r = requests.get(BASE_URL + ext, headers=header, params=params)
            r.raise_for_status()
            res = r.json()
            for item in res['items']:
                yield get_object_details(item)

def format_size(size):
    ''' convert filesize in bytes to human-readable form (e.g. 3.5G)
    '''
    codes = ' KMGTPE'
    factor = 0
    while factor <= len(codes):
        scaled = size / (1024 ** factor)
        if scaled < 1024:
            scaled = f'{scaled:.1f}' if 0 < scaled < 10 else f'{int(round(scaled))}'
            return f'{scaled}{codes[factor].strip()}'
        factor += 1
    
    scaled = f'{scaled:.1f}' if 0 < scaled < 10 else f'{int(round(scaled))}'
    return f'{scaled}{codes[factor].strip()}'

def ls_wrapper(args):
    ''' converts the CLI args to a function call for listiong file/folder contents
    '''
    if args.FILE is not None and not str(args.FILE).startswith('/'):
        sys.stderr.write(f'filepath must begin with "/": {args.FILE}')
        sys.exit(1)
    
    try:
        for item in list_files(args.FILE, args.pattern):
            if not args.all and item["name"].startswith('.'):
                continue
            line = [item["path"]]
            if args.l:
                line += [item["id"].lower(), format_size(item["size"]), item['created_date']]
            sys.stdout.write('\t'.join(line) + '\n')
    except ValueError:
        # only raises ValueError if the file/folder does not exist. If you look
        # for an empty folder, this will not be used.
        sys.stderr.write(f'cannot access {args.FILE}: No such file or directory')
        sys.exit(1)

def get_file(path: str):
    ''' get a file contents (streamed in chunks of 1600 bytes)
    '''
    project_id = get_project_id()
    data = get_data(path)
    if data is None:
        raise ValueError(f'cannot access data at {path}')
    data = list(data)
    if len(data) > 1:
        raise ValueError(f'too many matches at {path}')
    
    data_id = data[0]['data']['id']
    
    ext = f'api/projects/{project_id}/data/{data_id}:createDownloadUrl'
    r = requests.post(BASE_URL + ext, headers=_ICA_HEADERS)
    r.raise_for_status()
    
    url = r.json()['url']
    r = requests.get(url, stream=True)
    r.raise_for_status()
    return r.iter_content(1600)

def download_file(args):
    ''' download a file from ICA storage
    '''
    try:
        # get_file returns a stream of bytes, which we simply write to stdout
        for x in get_file(args.PATH):
            sys.stdout.buffer.write(x)
    except ValueError as err:
        sys.stderr.write(err.args[0] + '\n')
        sys.exit(1)
    except BrokenPipeError:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        sys.exit(0)

def get_upload_folder(path: Path | str | None, overwrite=False) -> Tuple[str, str]:
    ''' find the details of a folder to upload data to
    
    Args:
        path: path to upload data to. This could be either full path with extension,
            a path to a folder, or 
            None                           - upload to root directory
            /FOLDER/SUBFOLDER              - need to find details of subfolder
            /FOLDER/SUBFOLDER/BASENAME.EXT - need to find details of subfolder
            
            These folders/paths may or may not exist already.
    
    Returns:
        a tuple of (folder_id, folder_path)
    '''
    if path is None:
        sys.stderr.write('uploading to root folder')
        data = next(get_data('/'))
        return data['data']['id'], data['data']['details']['path']
    
    try:
        data = next(get_data(path))
        datatype = data['data']['details']['dataType']
        if datatype == 'FOLDER':
            return data['data']['id'], data['data']['details']['path']
        elif datatype == 'FILE':
            if overwrite:
                delete_file(data['data']['details']['path'])
                raise ValueError
                # raise NotImplementedError('add code for overwriting')
            else:
                # can't raise ValueError here, otherwise the catch handler would
                # try the parent folder
                raise RuntimeError(f'would overwrite existing file: {path}, use -f/--force to overwrite')
        else:
            raise TypeError(f'unknown datatype: {datatype}')
    except ValueError:
        # if we've tried a full path that does not exist, get the parent folder
        data = next(get_data(Path(path).parent))
        datatype = data['data']['details']['dataType']
        if datatype == 'FOLDER':
            return data['data']['id'], data['data']['details']['path']
        else:
            raise ValueError(f'unknown folder: {path}')
    except RuntimeError as e:
        raise ValueError(*e.args)

def get_upload_name(infile, destination: Path | str, folder: Path | str):
    ''' determine what the name of the file will be on ICA storage
    '''
    name = Path(destination).relative_to(Path(folder))
    if str(name) != '.':
        return str(name)
    elif type(infile) == str:
        return Path(infile).name
    else:
        raise ValueError(f'cannot determine filename to save as from --path ({destination})')

def upload_file(infile: str | io.BufferedReader | bytes, destination: Path, overwrite=False):
    ''' upload a file to ICA
    
    Args:
        infile: path to a file to upload, or a file handle for reading (e.g. open 
            file or sys.stdin), or byte sequence to upload
        destination: path to save data to on ICA. Can either be a folder (in
            which case the written file uses have the infile name), or a 
            complete file path.
        overwrite: whether to overwrite if file exists already
    '''
    project_id = get_project_id()
    folder_id, folder_path = get_upload_folder(destination, overwrite)
    upload_name = get_upload_name(infile, destination, folder_path)
    
    body = {'name': upload_name,
            'folderId': folder_id
            }
    body = json.dumps(body)
    
    ext = f'api/projects/{project_id}/data:createFileWithUploadUrl'
    try:
        r = requests.post(BASE_URL + ext, data=body, headers=_ICA_HEADERS)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            if overwrite:
                delete_file(upload_name)
                r = requests.post(BASE_URL + ext, data=body, headers=_ICA_HEADERS)
                r.raise_for_status()
            else:
                raise ValueError(f'error: file already exists at {folder_path}{upload_name}')
        raise e
    
    if type(infile) == str:
        infile = open(infile, 'rb')

    url = r.json()['uploadUrl']
    try:
        r = requests.put(url, data=infile, stream=True, headers=_ICA_HEADERS)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            raise ValueError
        raise e

def upload_wrapper(args):
    ''' upload a file to ICA storage
    '''
    infile = args.INFILE
    if infile is None:
        # read stdin into memory. This might use too much memory with large inputs.
        # TODO: figure out how to use sys.stdin directly, upload stdin directly
        # TODO: via stream. Currently using sys.stdin raises an error. Fixing this
        # TODO: would permit simple moving/coping of files by downloading and 
        # TODO: re-uploading.
        infile = sys.stdin.buffer.read()
    
    try:
        # get_file returns a stream of bytes, which we simply write to stdout
        upload_file(infile, args.path, args.force)
    except ValueError as err:
        sys.stderr.write(err.args[0] + '\n')
        sys.exit(1)
