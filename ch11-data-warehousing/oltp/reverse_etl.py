#!/usr/bin/python
import os
import csv
import MySQLdb

mydb = MySQLdb.connect(user="root", host="127.0.0.1", db="oltp")
cur = mydb.cursor()


with open(os.path.join(os.path.expanduser('~'), 'ml-100k/u.genre'), 'r') as genre_csv:
	genre_file = csv.reader(genre_csv, delimiter='|')
	for row in genre_file:
		if len(row) > 0:
			cur.execute("INSERT INTO genre(id, name) VALUES (%s, %s)", (row[0], row[1]))

occupation_id = 0
occupation_dict = dict()
with open(os.path.join(os.path.expanduser('~'), 'ml-100k/u.occupation'), 'r') as occupation_file:
	for occupation in [element.strip() for element in occupation_file]:
		cur.execute("INSERT INTO occupation(id, name) VALUES (%s, %s)", (occupation_id, occupation))
		occupation_dict[occupation] = occupation_id
		occupation_id += 1

with open(os.path.join(os.path.expanduser('~'), 'ml-100k/u.user'), 'r') as user_csv:
	user_file = csv.reader(user_csv, delimiter='|')
	for row in user_file:
		occupation_id = occupation_dict[row[3]]
		# throw exception if occupation_id is null
		cur.execute("INSERT INTO user(id, age, gender, occupation_id, zipcode, last_modified) VALUES (%s, %s, %s, %s, %s, %s)", (row[0], row[1], row[2], occupation_id, row[4], 'NOW()'))



#cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='movielens' AND TABLE_NAME='movies'");

#resultset = cur.fetchall()