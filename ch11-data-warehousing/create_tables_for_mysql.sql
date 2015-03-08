-- MySQL compatible file for reading data
CREATE SCHEMA IF NOT EXISTS movielens;
use movielens;

-- users table
-- zipcode is string instead of int, because we don't want zipcodes starting with 0 to be stripped of it.
CREATE TABLE IF NOT EXISTS users(user_id INT PRIMARY KEY, age INT NOT NULL, gender CHAR(1) NOT NULL, occupation VARCHAR(64) NOT NULL, zipcode VARCHAR(5)  NOT NULL);

LOAD DATA LOCAL INFILE '~/ml-100k/u.user' INTO TABLE users FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n';

-- movies table
-- Genre's generated via a script like this:
-- for gen in {'unknown','action','adventure','animation','childrens','comedy','crime','documentary','drama','fantasy','film_noir','horror','musical','mystery','romance','sci_fi','thriller','war','western'}; do printf "genre_$gen BOOL NOT NULL, "; done
CREATE TABLE IF NOT EXISTS movies(movie_id INT PRIMARY KEY, title VARCHAR(128) NOT NULL, release_date DATE NOT NULL, video_release_date DATE, imdb_url VARCHAR(1024), genre_unknown BOOL NOT NULL, genre_action BOOL NOT NULL, genre_adventure BOOL NOT NULL, genre_animation BOOL NOT NULL, genre_childrens BOOL NOT NULL, genre_comedy BOOL NOT NULL, genre_crime BOOL NOT NULL, genre_documentary BOOL NOT NULL, genre_drama BOOL NOT NULL, genre_fantasy BOOL NOT NULL, genre_film_noir BOOL NOT NULL, genre_horror BOOL NOT NULL, genre_musical BOOL NOT NULL, genre_mystery BOOL NOT NULL, genre_romance BOOL NOT NULL, genre_sci_fi BOOL NOT NULL, genre_thriller BOOL NOT NULL, genre_war BOOL NOT NULL, genre_western BOOL NOT NULL);

LOAD DATA LOCAL INFILE '~/ml-100k/u.item' INTO TABLE movies FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' (movie_id, title, @col3, @col4, imdb_url, genre_unknown, genre_action, genre_adventure, genre_animation, genre_childrens, genre_comedy, genre_crime, genre_documentary, genre_drama, genre_fantasy, genre_film_noir, genre_horror, genre_musical, genre_mystery, genre_romance, genre_sci_fi, genre_thriller, genre_war, genre_western) SET release_date = str_to_date(@col3,'%d-%b-%Y'), video_release_date = IF(@col4='', NULL, str_to_date(@col4, '%d-%b-%Y'));

-- ratings table
CREATE TABLE IF NOT EXISTS ratings (user_id INT NOT NULL, movie_id INT NOT NULL, rating INT NOT NULL, time TIMESTAMP NOT NULL);

LOAD DATA LOCAL INFILE '~/ml-100k/u.data' INTO TABLE ratings FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' (user_id, movie_id, rating, @time) SET time = FROM_UNIXTIME(@time);

