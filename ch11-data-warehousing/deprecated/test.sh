#!/bin/bash
hive -e "
CREATE EXTERNAL TABLE avg_movie_rating2(movie_id INT, rating DOUBLE)
STORED AS AVRO
LOCATION '/data/movielens/aggregated_ratings'"

hive -e "
INSERT INTO avg_movie_rating2
SELECT
  movie_id,
  ROUND(avg(rating), 1) as rating
FROM
  user_rating_part
GROUP BY
  movie_id"

sqoop export --connect jdbc:mysql://mgrover-haa-2.vpc.cloudera.com:3306/movie_dwh --username root --table avg_movie_rating --export-dir /data/movielens/aggregated_ratings -m 16 --update-key movie_id --update-mode allowinsert
