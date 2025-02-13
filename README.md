### icapy
A small python package to add some CLI commands to help working with ICA 
(illumina connected analytics), see API details here: 
https://ica.illumina.com/ica/api/swagger/index.htm

This is entirely unsupported, untested, subject to break if the ICA API changes,
and for my own convenience to help with finding and accessing ICA data.

### Install
```sh
pip install icacli
```

### Purpose
This provides an `ica` command line tool, which should be available immediately
after installation. You can run `ica --help` to get the full list of subcommands,
get help for each subcommand with e.g. `ica ls --help`

 - `ica ls`: list files/folders on ICA
 - `ica select`: choose which ICA project to use
 - `ica download`: download data from ICA
 - `ica upload`: upload data to ica
 - `ica rm`: delete data files or folders
 - `ica jobs`: list running jobs

Some other commands that would be nice to have, but are not implemented: 
`ica mkdir`, `ica cp`, `ica mv` and `ica run`
