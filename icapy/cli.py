

import argparse
import os
from pathlib import Path
import sys

from icapy.data import (ls_wrapper,
                        download_file,
                        upload_wrapper,
                        rm_wrapper,
                        )
from icapy.jobs import find_jobs
from icapy.projects import set_default_project

def CLI():
    ''' small CLI application to run ICA commands
    '''
    parser = argparse.ArgumentParser(description="small CLI application to run ICA commands")
    subparsers = parser.add_subparsers()
    
    login = subparsers.add_parser('select', help="set default project")
    login.set_defaults(func=set_default_project)
    
    ls = subparsers.add_parser('ls', help="list file/folder details")
    ls.add_argument('FILE', nargs='?', type=Path, help='path to file/folder')
    ls.add_argument('--pattern', nargs='?', help='search pattern (if ls-ing a folder)')
    ls.add_argument('-l', default=False, 
                    action='store_true', help='use a long listing format')
    ls.add_argument('-a', '--all', default=False, 
                    action='store_true', help='show hidden files')
    ls.set_defaults(func=ls_wrapper)
    
    download = subparsers.add_parser('download', help="download file")
    download.add_argument('PATH', type=Path, help='path to file')
    download.set_defaults(func=download_file)
    
    upload = subparsers.add_parser('upload', help="upload file")
    upload.add_argument('INFILE', nargs='?', help='path to local file, tries stdin if not used')
    upload.add_argument('--path', type=Path, help='path to destination file (full path or folder)')
    upload.add_argument('-f', '--force', default=False, action='store_true',
                        help='whether to overwrite if detination file exists')
    upload.set_defaults(func=upload_wrapper)
    
    rm = subparsers.add_parser('rm', help="remove (unlink) the FILE(s)")
    rm.add_argument('PATH', type=Path, help='path to file/folder')
    rm.add_argument('-r','--recursive', default=False, action='store_true',
                    help='remove directories and their contents')
    rm.set_defaults(func=rm_wrapper)
    
    jobs = subparsers.add_parser('jobs', 
                                 help="find analyses/jobs",
                                 description="Jobs are sorted by most " \
                                              "recently submitted first")
    jobs.add_argument('status', nargs='?',
                      help='optionally filter by job status. If used, must be ' \
                           'one of aborted, running, failed, requested, succeeded.')
    jobs.add_argument('--id',
                      help='To get details of a specific job. This overrides ' \
                           'any status requirement.')
    jobs.add_argument('--tag', nargs='*', help='tag to filter on.')
    jobs.add_argument('--max-jobs', type=int, default=5000,
                      help='Number of jobs to get (default=5000).')
    jobs.set_defaults(func=find_jobs)
    
    args = parser.parse_args()
    if not hasattr(args, 'func'):
        parser.print_help()
    else:
        try:
            args.func(args)
        except (KeyboardInterrupt, BrokenPipeError):
            devnull = os.open(os.devnull, os.O_WRONLY)
            os.dup2(devnull, sys.stdout.fileno())
            sys.exit(0)
