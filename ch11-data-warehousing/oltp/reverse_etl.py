#!/usr/bin/python
import os
import csv
import datetime
import time
import MySQLdb

from contextlib import closing

mydb = MySQLdb.connect(user="root", host="127.0.0.1", db="oltp")


with closing(mydb.cursor()) as cur:
	with open(os.path.join(os.path.expanduser('~'), 'ml-100k/u.genre'), 'r') as genre_csv:
		genre_file = csv.reader(genre_csv, delimiter='|')
		for row in genre_file:
			if len(row) > 0:
				cur.execute("INSERT INTO genre(id, name) VALUES (%s, %s)", (row[1], row[0]))

	occupation_id = 0
	occupation_dict = dict()
	with open(os.path.join(os.path.expanduser('~'), 'ml-100k/u.occupation'), 'r') as occupation_file:
		for occupation in [element.strip() for element in occupation_file]:
			cur.execute("INSERT INTO occupation(id, occupation) VALUES (%s, %s)", (occupation_id, occupation))
			occupation_dict[occupation] = occupation_id
			occupation_id += 1

	now = datetime.datetime.now()
	last_modified = now.strftime('%Y-%m-%d %H:%M:%S')
	with open(os.path.join(os.path.expanduser('~'), 'ml-100k/u.user'), 'r') as user_csv:
		user_file = csv.reader(user_csv, delimiter='|')
		for row in user_file:
			occupation_id = occupation_dict[row[3]]
			# throw exception if occupation_id is null
			cur.execute("INSERT INTO user(id, age, gender, occupation_id, zipcode, last_modified) VALUES (%s, %s, %s, %s, %s, %s)", (row[0], row[1], row[2], occupation_id, row[4], last_modified))


	with open(os.path.join(os.path.expanduser('~'), 'ml-100k/u.item'), 'r') as movie_csv:
		movie_file = csv.reader(movie_csv, delimiter='|')
		for row in movie_file:
	                release_date = None
	                if row[2]:
	                        release_date = time.strftime('%Y-%m-%d', time.strptime(row[2], "%d-%b-%Y"))		
			video_release_date = None
			if row[3]:
				video_release_date = time.strftime('%Y-%m-%d', time.strptime(row[3], "%d-%b-%Y"))
			cur.execute("INSERT INTO movie(id, title, release_date, video_release_date, imdb_url) VALUES (%s, %s, %s, %s, %s)", (row[0], row[1], release_date, video_release_date, row[4]))
			insert_statement = ''
			for x in xrange(5, 24):
				if row[x] == "1":
					if insert_statement:
						insert_statement += ","
					# (movie_id,
					insert_statement += "(%s," % row[0]
					# genre_id)
					insert_statement += str(x-5) + ")"
			if insert_statement:
				cur.execute("INSERT INTO movie_genre(movie_id, genre_id) VALUES {0}".format(insert_statement))

	with open(os.path.join(os.path.expanduser('~'), 'ml-100k/u.data'), 'r') as rating_csv:
		rating_file = csv.reader(rating_csv, delimiter='\t')
		for row in rating_file:
			cur.execute("INSERT INTO user_rating(timestamp, user_id, movie_id, rating) VALUES (FROM_UNIXTIME({0}), {1}, {2}, {3})".format(row[3], row[0], row[1], row[2]))

mydb.commit()
mydb.close()
