#!/bin/bash
# Steps in order to get a local PostgreSQL instance set up for testing (On RHEL6)
sudo yum install postgresql-libs postgresql postgresql-server
sudo cp /var/lib/pgsql/data/pg_hba.conf /var/lib/pgsql/data/pg_hba.conf~
sudo sed -i -e '/all/s/ident/trust/g' /var/lib/pgsql/data/pg_hba.conf
sudo service postgresql start
sudo -i -u postgres createdb oltp
sudo -i -u postgres psql -d oltp -c 'CREATE SCHEMA movielens'
# See documentation at http://www.postgresql.org/docs/9.1/static/app-createuser.html for cmd line options
sudo -i -u postgres createuser -R -S -i -d -w movielens
sudo -i -u postgres psql -d oltp -c 'GRANT ALL ON SCHEMA movielens TO movielens'

-- Run commands as 
-- psql -d oltp -U movielens -c 'COMMAND'
