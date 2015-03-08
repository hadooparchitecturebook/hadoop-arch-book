#!/bin/bash
sudo yum install MySQL-python
# For Mac OSX, I followed http://stackoverflow.com/questions/1448429/how-to-install-mysqldb-python-data-access-library-to-mysql-on-mac-os-x
# I also had to source out ARCHFLAGS environment variable when calling build and install commands, so I did something like:
# sudo ARCHFLAGS=-Wno-error=unused-command-line-argument-hard-error-in-future python setup.py install
