#!/usr/bin/python
import MySQLdb

mydb = MySQLdb.connect(user="root", host="127.0.0.1", db="movielens")
cur = mydb.cursor()

cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='movielens' AND TABLE_NAME='movies'");

resultset = cur.fetchall()

genres = [elem[0].replace("genre_", "") for elem in resultset if "genre_" in elem[0]]

for row in genres :
	print row
