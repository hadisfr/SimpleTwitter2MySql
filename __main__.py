#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import absolute_import, print_function

import MySQLdb
import sys
import tweepy
import json
import time

from userpass import *
db_name = "tweet_mine"
db_host = "localhost"
filtered_words = ["basketball", "tweet"]

tweets_table_name = "tweets"
users_table_name = "users"

counter = 0;

def open_database(cursor):
	try:
		cursor.execute('SET NAMES utf8mb4')
		cursor.execute('SET CHARACTER SET utf8mb4')
		cursor.execute('SET character_set_connection=utf8mb4')
		cursor.execute("use " + db_name + ";")
	except MySQLdb.OperationalError as ex:
		if ex.args[0] == 1049:
			print("creating database " + db_name, file = sys.stderr)
			cursor.execute("create database " + db_name + " character set UTF8MB4;")
			cursor.execute("use " + db_name + ";")
		else:
			raise

def prepare_tables(cursor):
	cursor.execute("show tables;")
	tables = [i[0] for i in cursor.fetchall()]
	if users_table_name not in tables:
		cursor.execute("create table " + users_table_name + "(\
			id bigint not null PRIMARY KEY,\
			name text,\
			screen_name text,\
			location text,\
			url text,\
			description text,\
			protected bit,\
			verified bit,\
			followers_count int,\
			friends_count int,\
			listed_count int,\
			favourites_count int,\
			statuses_count int,\
			json text\
			);")
	#TODO: add created_at datetime to table.
	if tweets_table_name not in tables:
		cursor.execute("create table " + tweets_table_name + "(\
			id bigint not null PRIMARY KEY,\
			text text,\
			source text,\
			user_id bigint not null,\
			geo text,\
			coordinates text,\
			place text,\
			retweet_count int,\
			favorite_count int,\
			lang text,\
			timestamp_ms bigint,\
			json text,\
			FOREIGN KEY (user_id) REFERENCES " + users_table_name + "(id)\
			);")
	#TODO add created_at datetime to table.

class StreamListener(tweepy.StreamListener):
	cursor = None

	def __init__(self, cursor):
		self.cursor = cursor
		super(StreamListener, self).__init__()

	def on_error(self, status):
		print(status)

	def on_disconnect(self):
		print("disconnected.", file = sys.stderr)

	def on_status(self, status):
		global counter
		counter += 1
		print(counter)
		try:
			text = status.extended_tweet["full_text"]
		except AttributeError:
			text = status.text
		text = text.replace("\'", "").replace("\"", "")
		# print("\n%s :\t%s" % (status._json["user"]["name"], text))
		# try:
		# 	text = status.extended_tweet._json
		# except AttributeError:
		# 	text = status._json
		# print(json.dumps(text, indent = 4, ensure_ascii=False))
		jo = status._json
		u = jo["user"]
		user_id = (u["id"],)
		user_data = (u["name"], u["screen_name"], u["location"], u["url"], u["description"], u["protected"], u["verified"], u["followers_count"], u["friends_count"], u["listed_count"], u["favourites_count"], u["statuses_count"], json.dumps(u))
		tweet_data = (jo["id_str"], text, jo["source"], u["id"], jo["geo"], jo["coordinates"], jo["place"], jo["retweet_count"], jo["favorite_count"], jo["lang"], jo["timestamp_ms"], json.dumps(jo))
		try:
			if not self.cursor.execute("select * from " + users_table_name + " where id=%s;", user_id):
				self.cursor.execute("insert into " + users_table_name + " values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", user_id + user_data)
			else:
				self.cursor.execute("update " + users_table_name + " set name=%s, screen_name=%s, location=%s, url=%s, description=%s, protected=%s,verified=%s, followers_count=%s, friends_count=%s, listed_count=%s, favourites_count=%s, statuses_count=%s, json=%s where id=%s;", user_data + user_id)
			self.cursor.execute("insert into " + tweets_table_name + " values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", tweet_data)
		except Exception as ex:
			print(ex, file=sys.stderr)

if __name__ == '__main__':
	try:
		db = MySQLdb.connect(user = db_username, passwd = db_password, host = db_host, use_unicode = True)
		cursor = db.cursor()
	except MySQLdb.OperationalError as ex:
		print("DB Connection Error:\t" + str(ex))
		exit()

	open_database(cursor)
	prepare_tables(cursor)
	db.autocommit("on")
	
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)

	stream = tweepy.Stream(auth, StreamListener(cursor))
	try:
		stream.filter(track= filtered_words)
	except KeyboardInterrupt as ex:
		pass
	print()
