CREATE SCHEMA IF NOT EXISTS oltp;
use oltp;

CREATE TABLE IF NOT EXISTS occupation (
  id INT PRIMARY KEY NOT NULL,
  occupation VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS user(
  id INT PRIMARY KEY NOT NULL,
  age INT NOT NULL,
  gender CHAR(1) NOT NULL,
  occupation_id INT NOT NULL,
  zipcode VARCHAR(5) NOT NULL,
  last_modified DATETIME NOT NULL,
  FOREIGN KEY(occupation_id) REFERENCES occupation(id)
);

CREATE TABLE IF NOT EXISTS genre(
  id INT NOT NULL PRIMARY KEY,
  name VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS movie(
  id INT PRIMARY KEY NOT NULL,
  title VARCHAR(128) NOT NULL,
  release_date DATE,
  video_release_date DATE,
  imdb_url VARCHAR(1024) NOT NULL
);

CREATE TABLE IF NOT EXISTS movie_genre(
  movie_id INT NOT NULL,
  genre_id INT NOT NULL,
  FOREIGN KEY(movie_id) REFERENCES movie(id),
  FOREIGN KEY(genre_id) REFERENCES genre(id)
);

CREATE TABLE IF NOT EXISTS user_rating(
  id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  timestamp DATETIME NOT NULL,
  user_id INT NOT NULL,
  movie_id INT NOT NULL,
  rating INT NOT NULL,
  FOREIGN KEY(user_id) REFERENCES user(id),
  FOREIGN KEY(movie_id) REFERENCES movie(id)
);
