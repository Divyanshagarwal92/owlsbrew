#!/usr/bin/python
import pymysql
import time
from lemmatizer import clean
from dictlib import count_dict_insert, threshold_dict

def load_hashtag_stopwords():

	f = open('hashtag_stopwords.txt');
	swords = f.readlines();
	f.close();

	stopwords = [];	
	for word in swords:
		stopwords.append(word[:-1]);

	return stopwords;


def load_stopwords():

	f = open('ENG_stopword.txt');
	swords = f.readlines();
	f.close();

	stopwords = [];	
	for word in swords:
		stopwords.append(word[:-1]);

	return stopwords;

def get_tweets_from_db( q, conn,cur, stopwords):
#Using a range of tweetIDs, fetches tweets from MySQL database
	tweets = []; 
	
	#Run query using the start and end IDs
	#sql_query = "SELECT ID_str, text FROM tweets WHERE ID_str >= " + startID + " AND ID_str <= " + endID;
	sql_query = "SELECT ID_str, text, hashtags FROM tweets_new WHERE search_string = '" + q + "'";
	#sql_query = "SELECT ID_str, text, hashtags FROM tweets_new";
	#print sql_query;
	cur.execute(sql_query);
	out = cur.fetchall();
	
	for item in out:
		if item[0] == '' or item[2] == '': 
		    continue;
		hashtags = item[2].split(',');
		hashtags = [x.lower() for x in hashtags];
		#newtext = [];
		#for token in text:
		#	cleaned_token = clean(token);
		#	if cleaned_token not in stopwords:
		#		newtext.append(cleaned_token);
		templs = [item[0], hashtags];
		tweets.append(templs);  
	#print tweets
    #Return tweet IDs and text in a list
	return tweets; 


def main(conn, cur):

	dict_tea = {};
	dict_cocktail = {};

	stopwords = load_stopwords();
	hashtag_stopwords = load_hashtag_stopwords();
	teatweets = get_tweets_from_db('tea', conn, cur, stopwords);	
	cocktailtweets = get_tweets_from_db('cocktail',conn, cur, stopwords);	
	for item in teatweets:
		hashtags = item[1];
		for token in hashtags:
			if token not in hashtag_stopwords:
				if token not in dict_tea:
					dict_tea[token] = 1/float(len(hashtags)*len(hashtags));
				else:
					dict_tea[token] += 1/float(len(hashtags)*len(hashtags));

	for item in cocktailtweets:
		hashtags = item[1];
		for token in hashtags:
			if token not in hashtag_stopwords:
				if token not in dict_cocktail:
					dict_cocktail[token] = 1/float(len(hashtags)*len(hashtags));
				else:
					dict_cocktail[token] += 1/float(len(hashtags)*len(hashtags));

	intersected_dict = {};

#	for key in dict_tea:
#		if key in dict_cocktail and key not in intersected_dict and key not in hashtag_stopwords:
#			intersected_dict[key] = dict_tea[key] + dict_cocktail[key];
#			
#	for key in dict_cocktail:
#		if key in dict_tea and key not in intersected_dict and key not in hashtag_stopwords:
#			intersected_dict[key] = dict_tea[key] + dict_cocktail[key];

	d = threshold_dict(dict_cocktail, 1);
	for item in d:
		print int(item[1]), item[0]
