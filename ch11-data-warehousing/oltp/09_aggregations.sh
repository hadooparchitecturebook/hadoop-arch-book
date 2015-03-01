#!/bin/bash
hive -e "
CREATE TABLE user_movie_count AS
SELECT
  movie_id,
  user_id,
  COUNT(*) AS count
FROM
  user_rating_fact
GROUP BY
  movie_id,
  user_id"

hive -e "
CREATE TABLE avg_movie_rating AS
SELECT
  movie_id,
  ROUND(AVG(rating), 1) AS rating
FROM
  user_rating_part
GROUP BY
  movie_id"

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
