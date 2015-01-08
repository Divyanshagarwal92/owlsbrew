import twitter_owlsbrew as tweets
import connections


if __name__ == '__main__':
	
	#connect to mysql database
	db_conn,db_cur = connections.connect_to_mysql()

	#connect to Amazon sqs
	#queue_name = 'TwitterMap_gp_2'
	#sqs_queue = connections.connect_to_sqs(queue_name)
	sqs_queue = '';

	#list of search strings
	search_strings = ['owlsbrew','theowlsbrew','owl\'s brew','owls brew','owl\'sbrew','theowl\'sbrew'];
	#search_strings = ['tea cocktails'];

	#For each search string, perform the following.
	# 1. Fetch data from twitter search api
	total_tweets = 0;	
	for q in search_strings:
		print "search string = %s"%q
		n = tweets.tweets_main(q,db_conn,db_cur,sqs_queue)
		
		print "***************************************"
