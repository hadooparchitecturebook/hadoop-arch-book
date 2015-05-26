#!/bin/bash -e
# Run on -4
# Need to copy Mysql connector jar to /var/lib/sqoop.
# See documentation http://www.cloudera.com/content/cloudera/en/documentation/core/v5-2-x/topics/cdh_ig_jdbc_driver_install.html

sudo -u hdfs hadoop fs -mkdir -p  /data/movielens
sudo -u hdfs hadoop fs -chown -R ${USER}: /data/movielens

sudo -u hdfs hadoop fs -mkdir -p /user/${USER}
sudo -u hdfs hadoop fs -chown -R ${USER}: /user/${USER}

# All nodes need to have access to the source database

# Cleanup if necessary. Script should continue even if
# this next rm command fails, hence the last || :
sudo -u hdfs hadoop fs -rm -r /data/movielens/movie || :

sqoop import --connect jdbc:mysql://mgrover-haa-2.vpc.cloudera.com:3306/oltp \
--username root --query \
'SELECT movie.*, group_concat(genre.name)
FROM movie 
JOIN movie_genre ON (movie.id = movie_genre.movie_id)
JOIN genre ON (movie_genre.genre_id = genre.id) 
WHERE ${CONDITIONS}
GROUP BY movie.id' \
--split-by movie.id --as-avrodatafile --target-dir /data/movielens/movie
