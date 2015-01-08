#!/usr/bin/python
import pymysql
import time
from alchemyapi_python.alchemyapi import AlchemyAPI

alchemyapi = AlchemyAPI()

wait_time = 5;	#Seconds to wait before doing another read
read_num = 1;	#Number of messages to read at once

def get_tweets_from_db(conn,cur):
#Using a range of tweetIDs, fetches tweets from MySQL database
	tweets = [];

	#Run query using the start and end IDs
	#sql_query = "SELECT ID_str, text FROM tweets WHERE ID_str >= " + startID + " AND ID_str <= " + endID;
	sql_query = "SELECT ID_str, text FROM tweets WHERE ID_str >= 541710557272113152";
	cur.execute(sql_query);
	out = cur.fetchall();

	for item in out:
		if item[0] == '':
			continue;
		templs = [item[0], item[1]];
		tweets.append(templs);	
#	print tweets
	#Return tweet IDs and text in a list
	return tweets;	

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

def update_tweets_in_db(tweets, analyzed_tweets,conn,cur):
#Updates tweets in MySQL database with the sentiment analysis output

	#SQL query to update tweet with the sentiment analysis output 
	for i in range(0,len(tweets)):
		polarity = analyzed_tweets[i][0].encode('ascii');	
		polarity_score = analyzed_tweets[i][1];	
		update_query = "UPDATE tweets SET polarity=\'" + polarity + "\', polarity_score=" + str(polarity_score) + " WHERE ID_str=" + tweets[i][0];
		print update_query;
		cur.execute(update_query);
		conn.commit();
	

def sentiment_main(conn,cur):

		tweets = get_tweets_from_db(conn, cur);
		print "Number of tweets in DB : ", len(tweets);
		analyzed_tweets = sentiment_analysis(tweets);
		if analyzed_tweets == 'daily-transaction-limit-exceeded':
			print "AlchemyAPI daily-transaction-limit-exceeded"
		else:
			update_tweets_in_db(tweets, analyzed_tweets,conn,cur);

