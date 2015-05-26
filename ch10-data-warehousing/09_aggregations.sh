#!/bin/bash
# This query creates a table that stores the number of times each user 
# rated a particular movie
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

# This query creates an aggregate table that stores 
# the average movie rating for all movies
hive -e "
CREATE TABLE avg_movie_rating AS
SELECT
  movie_id,
  ROUND(AVG(rating), 1) AS rating
FROM
  user_rating_part
GROUP BY
  movie_id"


# This query finds the highest trending movies in the last 7 days
#TODAY=$(date "+%s")
# Need to do this because the ratings are from back in the day
TODAY="892151366"
LAST_WEEK_IN_MS=$((($TODAY-86400*7)*1000))

hive -e \
"SELECT
  movie_id,
  diff
FROM (
  SELECT
    this_week.movie_id,
    (this_week.rating - last_week.rating) as diff
  FROM
    avg_movie_rating this_week
  JOIN (
    SELECT
      movie_id,
      ROUND(AVG(rating), 1) AS rating
    FROM
      user_rating_fact
    WHERE
      timestamp <= $LAST_WEEK_IN_MS
    GROUP BY
      movie_id
  )last_week
  ON (this_week.movie_id = last_week.movie_id)
)t
ORDER BY
  diff DESC
  LIMIT 10"
