#!/usr/bin/python
import MySQLdb

mydb = MySQLdb.connect(user="root", host="127.0.0.1", database="movielens")
cur = db.cur()

cur.execute("SELECT * from users limit 5");

for row in cur.fetchall() :
	print row[0]