#!/bin/bash
sqoop export --connect dbc:mysql://mysql_server:3306/movieDWH --username myuser --password mypass --table aggregated_ratings --export-dir /data/movielens/aggregated_ratings -m 16 --update-key user_id, movie_id --staging-table staging_rating
