#!/bin/bash
hive -e "
CREATE TABLE user_movie_count AS
SELECT
  movie_id,
  user_id,
  COUNT(*)
FROM
  user_rating_fact
GROUP BY
  movie_id,
  user_id"

hive -e "
CREATE TABLE avg_movie_rating AS
SELECT
  movie_id,
  ROUND(AVG(rating), 1)
FROM
  user_rating_part
GROUP BY
  movie_id"
