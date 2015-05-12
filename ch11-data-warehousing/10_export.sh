#!/bin/bash
# The destination table in DWH has to exist before this command is run
# along with appropriate write permissions to the user running the sqoop job
# Something like this should do:
# CREATE TABLE avg_movie_rating(movie_id INT, rating DOUBLE);

sqoop export --connect \
jdbc:mysql://mgrover-haa-2.vpc.cloudera.com:3306/movie_dwh --username root \
--table avg_movie_rating --export-dir /user/hive/warehouse/avg_movie_rating \
-m 16 --update-key movie_id --update-mode allowinsert \
--input-fields-terminated-by '\001' --lines-terminated-by '\n'
