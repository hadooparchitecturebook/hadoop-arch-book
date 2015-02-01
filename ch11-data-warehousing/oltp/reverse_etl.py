#!/usr/bin/python
import MySQLdb

mydb = MySQLdb.connect(user="root", host="127.0.0.1", db="movielens")
cur = mydb.cursor()

cur.execute("SELECT * from users limit 5");

for row in cur.fetchall() :
	print row
