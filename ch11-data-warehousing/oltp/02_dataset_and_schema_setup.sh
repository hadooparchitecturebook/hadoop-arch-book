#!/bin/bash
cd ~
wget http://files.grouplens.org/datasets/movielens/ml-100k.zip
unzip ml-100k.zip
cd -

psql -d oltp -U movielens -f create_tables.sql
