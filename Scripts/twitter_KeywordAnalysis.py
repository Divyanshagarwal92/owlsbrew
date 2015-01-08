#!/usr/bin/python
import boto.sqs
import pymysql
import time
import re
import sys
from alchemyapi_python.alchemyapi import AlchemyAPI

alchemyapi = AlchemyAPI()

def load_stopwords():
	
	f = open('keywordext_stopwords.txt');
	s = f.readlines();
	f.close();

	return [x[:-1] for x in s];
		
def get_tweets_from_db(conn,cur):
#Using a range of tweetIDs, fetches tweets from MySQL database
	tweets = [];

	#Run query using the start and end IDs
	#sql_query = "SELECT ID_str, text FROM tweets WHERE ID_str >= " + startID + " AND ID_str <= " + endID;
	sql_query = "SELECT ID_str, text, username FROM tweets";
	cur.execute(sql_query);
	out = cur.fetchall();

	for item in out:
		if item[0] == '':
			continue;
		templs = [item[0], item[1], item[2]];
		tweets.append(templs);	
	#Return tweet IDs and text in a list
	return tweets;	

def keywordExtraction(posts, stopwords):
	sentence = ''
	newpost = '';
	newposts = [];
	for post in posts:
		newpost = '';
		if not re.match('RT \w.*: \w.*',post):
			tokens = post.split();
			for token in tokens:
				if token.lower() not in stopwords and token[0:4] != 'http':
					newpost += token + ' ';
			newposts.append(newpost[:-1]);
		else: continue;
	posts = list(set(newposts));
	for post in posts:
		sentence += '. ' + post;
	print sentence;
	response = alchemyapi.keywords('text', sentence, {'keywordExtractMode':'strict', 'maxRetrieve': 100} )
	print('## Keywords ##')
	#print response
	for keyword in response['keywords']:
		print('text: ', keyword['text'].encode('utf-8'))
		print('relevance: ', keyword['relevance'])
		print('')

def sentiment_analysis(tweetls):
	#tweetls is a list of tuples of the form (ID, text)
	#Outputs a list of tuples of the form (ID, text, polarity, polarity_score)

	sent_tweets = []
	for tweet in tweetls:
		response = alchemyapi.sentiment("text", tweet[1])
		if response['status'] == 'ERROR':
			if response['statusInfo'] == 'daily-transaction-limit-exceeded':
				error_msg = 'daily-transaction-limit-exceeded'
				return error_msg
			else:	
				sent_tweets.append([ 'unknown', 0.0 ])
				continue

		type = response["docSentiment"]["type"]
		
		if type.encode('ascii') == 'neutral':
			sent_tweets.append([ type, 0.0 ])
		else:
			sent_tweets.append([ type, response["docSentiment"]["score"] ])

	return sent_tweets

def main(conn,cur):

		#tweets = get_tweets_from_db(conn, cur);
		#print "Number of tweets in DB : ", len(tweets);

		f = open('mentions.txt');	
		input = f.readlines();
		f.close();

		tweets = [];	
		for item in input:
			tweets.append(item[1:-2]);	

		stopwords = load_stopwords();		
		print stopwords;
		#owlsbrew_tweets = [];
		#other_tweets = []	
		#for tweet in tweets:
		#	if tweet[2] == 'TheOwlsBrew':
		#		owlsbrew_tweets.append(tweet);
		#	else:
		#		other_tweets.append(tweet);

		keywordExtraction(tweets, stopwords);
